"""Local Discord interaction route helpers for the API server.

This module is intentionally small and fail-closed. It only builds the
server-side shape needed to receive Discord interaction callbacks safely:
read raw bytes, verify timestamp/signature headers first, reject replayed
requests, then build a fast ephemeral queue ACK. It does not register a Discord
endpoint, send network messages, or apply approval decisions.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol, cast


def _aiohttp_web() -> Any:
    return importlib.import_module("aiohttp.web")


DISCORD_INTERACTION_ROUTE = "/discord/interactions/soma"
DISCORD_SIGNATURE_HEADER = "X-Signature-Ed25519"
DISCORD_TIMESTAMP_HEADER = "X-Signature-Timestamp"
DISCORD_TIMESTAMP_MAX_AGE_SECONDS = 300
DISCORD_TIMESTAMP_MAX_FUTURE_SKEW_SECONDS = 5
DISCORD_REPLAY_CACHE_TTL_SECONDS = DISCORD_TIMESTAMP_MAX_AGE_SECONDS + DISCORD_TIMESTAMP_MAX_FUTURE_SKEW_SECONDS

_ALLOWED_PUBLIC_KEY_ENV_NAMES = {"DISCORD_APPLICATION_PUBLIC_KEY"}
_BLOCKED_PUBLIC_KEY_ENV_MARKERS = (
    "TOKEN",
    "SECRET",
    "WEBHOOK",
    "TELEGRAM",
    "AUTHORIZATION",
    "BEARER",
)
_CUSTOM_ID_RE = re.compile(r"^mim:soma-review:v1:(approve|reject|defer):([A-Za-z0-9][A-Za-z0-9_.:-]{0,127})$")
_SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_ASCII_DECIMAL_RE = re.compile(r"^[0-9]{1,20}$")
_ALLOWED_ENGINE_NAMES = {"preview", "mim-preview", "mim_preview"}
_RUNNER_CONTENT_MAX_LENGTH = 512
_FEEDBACK_KIND = "discord_interaction_feedback"
_FEEDBACK_SOURCE = "discord_interaction_livegate"
_FEEDBACK_ENDPOINT_VERSION = "soma-v1"
_FEEDBACK_RESULTS = {"preview_ack", "duplicate"}
_WORK_ITEM_KIND = "discord_interaction_work_item"
_WORK_ITEM_STATUSES = {"queued", "duplicate"}


class DiscordInteractionFeedbackSink(Protocol):
    """Append-only destination for safe interaction feedback events."""

    def record(self, event: "DiscordInteractionFeedbackEvent") -> str:
        """Append the event and return `recorded` or `duplicate`."""


@dataclass(frozen=True)
class DiscordInteractionFeedbackEvent:
    """Sanitized append-only event produced after a safe Discord ACK preview.

    This is the first MIM learning-loop artifact. It intentionally stores only
    the decision contract and deterministic idempotency key. Raw headers,
    signatures, bot tokens, and full Discord payloads are not part of the event.
    """

    event_id: str
    idempotency_key: str
    timestamp: str
    interaction_id: str
    action: str
    review_id: str
    source: str
    endpoint_version: str
    result: str
    kind: str = _FEEDBACK_KIND
    signature: str | None = None
    headers: dict[str, Any] | None = None


@dataclass
class DiscordInteractionJsonlFeedbackSink:
    """Append-only JSONL sink with local idempotency tracking.

    This writes a local artifact only. It does not open or mutate a runtime DB.
    Duplicate clicks append a second sanitized row marked `duplicate`, so the log
    remains append-only while downstream consumers can collapse by key.
    """

    path: Path | str
    _seen_keys: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8", errors="ignore").splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = row.get("idempotency_key")
                if isinstance(key, str):
                    self._seen_keys.add(key)

    def record(self, event: DiscordInteractionFeedbackEvent) -> str:
        event_result = "duplicate" if event.idempotency_key in self._seen_keys else event.result
        event_to_write = DiscordInteractionFeedbackEvent(**{**asdict(event), "result": event_result})
        ok, code = validate_discord_feedback_event(event_to_write)
        if not ok:
            return code
        self._seen_keys.add(event_to_write.idempotency_key)
        row = _feedback_event_to_json_row(event_to_write)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
        return event_result if event_result == "duplicate" else "recorded"


class DiscordInteractionWorkQueueSink(Protocol):
    """Append-only destination for later work outside the HTTP callback."""

    def enqueue(self, item: "DiscordInteractionWorkItem") -> str:
        """Append the work item and return `queued` or `duplicate`."""


@dataclass(frozen=True)
class DiscordInteractionWorkItem:
    """Sanitized queue item for a later worker outside the callback path.

    It carries only the reviewed decision contract. It cannot claim runtime DB
    writes, shell execution, env lookup, or live sends from the callback.
    """

    work_id: str
    idempotency_key: str
    timestamp: str
    interaction_id: str
    action: str
    review_id: str
    source: str
    endpoint_version: str
    status: str
    kind: str = _WORK_ITEM_KIND
    runtime_write: bool = False
    live_send: bool = False
    shell: bool = False
    env_lookup: bool = False
    signature: str | None = None
    headers: dict[str, Any] | None = None


@dataclass
class DiscordInteractionJsonlWorkQueueSink:
    """Append-only JSONL work queue for future async processing.

    This is a local artifact seam only. A later worker may read it, but the
    Discord callback never runs that worker inline.
    """

    path: Path | str
    _seen_keys: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8", errors="ignore").splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = row.get("idempotency_key")
                if isinstance(key, str):
                    self._seen_keys.add(key)

    def enqueue(self, item: DiscordInteractionWorkItem) -> str:
        status = "duplicate" if item.idempotency_key in self._seen_keys else item.status
        item_to_write = DiscordInteractionWorkItem(**{**asdict(item), "status": status})
        ok, code = validate_discord_work_item(item_to_write)
        if not ok:
            return code
        self._seen_keys.add(item_to_write.idempotency_key)
        row = _work_item_to_json_row(item_to_write)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
        return status


@dataclass(frozen=True)
class DiscordInteractionWorkQueueCandidate:
    """Read-only candidate preview derived from queued button signals."""

    review_label: str
    action_counts: dict[str, int] = field(default_factory=dict)
    queued: int = 0
    duplicates: int = 0
    state_preview: str = "needs_review"
    weight_preview: int = 0
    apply_candidate: str = "review_needed"
    warnings: tuple[str, ...] = ()
    latest_timestamp: str | None = None


@dataclass(frozen=True)
class DiscordInteractionWorkQueueSummary:
    """Read-only aggregate view of the local interaction work queue."""

    path: str
    exists: bool
    total_rows: int = 0
    valid_items: int = 0
    invalid_rows: int = 0
    queued: int = 0
    duplicates: int = 0
    action_counts: dict[str, int] = field(default_factory=dict)
    review_counts: dict[str, int] = field(default_factory=dict)
    latest_timestamp: str | None = None
    candidates: list[DiscordInteractionWorkQueueCandidate] = field(default_factory=list)


@dataclass(frozen=True)
class DiscordInteractionEngineResult:
    """ACK plus explicit side-effect claims from a local wrapper engine."""

    ack: dict[str, Any] | None
    engine_name: str = "preview"
    live_send: bool = False
    runtime_write: bool = False
    applied: bool = False


@dataclass(frozen=True)
class DiscordInteractionRunnerInput:
    """Validated component data passed to a preview runner.

    This intentionally omits the raw Discord payload so a future Hermes/Codex/
    Claude/OpenCode wrapper receives only the button decision contract, not
    headers, signatures, tokens, or arbitrary interaction JSON.
    """

    action: str
    review_id: str
    interaction_type: int
    interaction_id: str


@dataclass(frozen=True)
class DiscordInteractionRunnerResult:
    """Typed preview-only ACK proposal from a local runner.

    The forbidden flags document the approval gate. A later live runner may add
    a separate contract, but this pre-gate runner cannot claim live sends,
    runtime writes, shell execution, or env/secret lookup.
    """

    content: str
    response_type: int = 4
    flags: int = 64
    live_send: bool = False
    runtime_write: bool = False
    shell: bool = False
    env_lookup: bool = False


class DiscordInteractionPreviewRunner(Protocol):
    """Preview-only agent runner contract used behind the engine seam."""

    def propose_ack(self, runner_input: DiscordInteractionRunnerInput) -> DiscordInteractionRunnerResult:
        """Return a typed local ACK proposal without side effects."""


class DiscordInteractionEngine(Protocol):
    """Safe seam for fork-local interaction engines.

    This protocol lets Q/MIM swap the local decision engine later without
    letting Discord config import arbitrary callables, shell out, or contact
    external agents from the callback route itself.
    """

    name: str

    def build_ack(self, payload: dict[str, Any]) -> DiscordInteractionEngineResult:
        """Return an ACK preview without live sends or runtime writes."""


@dataclass(frozen=True)
class PreviewDiscordInteractionEngine:
    """Default preview-only engine before any live apply gate exists."""

    name: str = "preview"
    runner: DiscordInteractionPreviewRunner | None = None

    def build_ack(self, payload: dict[str, Any]) -> DiscordInteractionEngineResult:
        if self.runner is None:
            return DiscordInteractionEngineResult(ack=build_discord_ack_preview(payload), engine_name=self.name)

        runner_input = build_discord_runner_input(payload)
        if runner_input is None:
            return DiscordInteractionEngineResult(ack=None, engine_name=self.name)
        try:
            runner_result = self.runner.propose_ack(runner_input)
        except Exception:
            return DiscordInteractionEngineResult(ack=None, engine_name=self.name, runtime_write=True)
        return runner_result_to_engine_result(runner_result, engine_name=self.name)


@dataclass(frozen=True)
class DiscordInteractionConfig:
    """Resolved local route config.

    `enabled=False` is the safe default. A route becomes active only when the
    API server config explicitly enables it and supplies a Discord application
    public key by value or by the allowlisted public-key environment variable.
    """

    enabled: bool = False
    public_key: str = ""
    route_path: str = DISCORD_INTERACTION_ROUTE
    engine_name: str = "preview"


@dataclass
class DiscordInteractionReplayCache:
    """Bounded in-memory replay guard for local Discord interaction callbacks.

    The first live-ready safety layer should not write to runtime SQLite or any
    external store. This cache only remembers request fingerprints in the API
    server process, prunes by TTL, and fails closed when full.
    """

    max_entries: int = 4096
    ttl_seconds: int = DISCORD_REPLAY_CACHE_TTL_SECONDS
    _entries: dict[str, float] = field(default_factory=dict)

    def _fingerprint(self, *, timestamp: str, signature: str, body: bytes) -> str:
        h = hashlib.sha256()
        h.update(timestamp.encode("ascii", errors="ignore"))
        h.update(b":")
        h.update(signature.encode("ascii", errors="ignore"))
        h.update(b":")
        h.update(body)
        return h.hexdigest()

    def _prune(self, now: float) -> None:
        expired = [key for key, expires_at in self._entries.items() if expires_at <= now]
        for key in expired:
            self._entries.pop(key, None)

    def check_and_remember(self, *, timestamp: str, signature: str, body: bytes, now: float | None = None) -> tuple[bool, str]:
        """Remember a verified request fingerprint or reject a duplicate.

        Returns `(True, "ok")` for the first sighting. Duplicate requests inside
        the TTL are rejected. If the cache is full after pruning, fail closed
        rather than evicting a still-valid fingerprint.
        """

        current = time.time() if now is None else float(now)
        self._prune(current)
        key = self._fingerprint(timestamp=timestamp, signature=signature, body=body)
        if key in self._entries:
            return False, "replay_detected"
        if len(self._entries) >= max(1, int(self.max_entries)):
            return False, "replay_cache_full"
        self._entries[key] = current + max(1, int(self.ttl_seconds))
        return True, "ok"


def _looks_like_secret_env(name: str) -> bool:
    upper = name.upper()
    return any(marker in upper for marker in _BLOCKED_PUBLIC_KEY_ENV_MARKERS)


def _safe_component(value: Any) -> bool:
    return isinstance(value, str) and _SAFE_COMPONENT_RE.fullmatch(value) is not None and not _contains_secret_marker(value)


def _utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _feedback_event_to_json_row(event: DiscordInteractionFeedbackEvent) -> dict[str, Any]:
    row = asdict(event)
    row.pop("signature", None)
    row.pop("headers", None)
    return row


def _work_item_to_json_row(item: DiscordInteractionWorkItem) -> dict[str, Any]:
    row = asdict(item)
    row.pop("signature", None)
    row.pop("headers", None)
    return row


def validate_discord_feedback_event(event: Any) -> tuple[bool, str]:
    """Validate that feedback rows contain only safe append-only fields."""

    if not isinstance(event, DiscordInteractionFeedbackEvent):
        return False, "invalid_feedback_event"
    if event.kind != _FEEDBACK_KIND:
        return False, "invalid_feedback_kind"
    if event.source != _FEEDBACK_SOURCE or event.endpoint_version != _FEEDBACK_ENDPOINT_VERSION:
        return False, "invalid_feedback_source"
    if event.signature or event.headers:
        return False, "unsafe_feedback_event"
    if event.action not in {"approve", "reject", "defer"}:
        return False, "invalid_feedback_action"
    if event.result not in _FEEDBACK_RESULTS:
        return False, "invalid_feedback_result"
    if not _safe_component(event.interaction_id) or not _safe_component(event.review_id):
        return False, "unsafe_feedback_event"
    expected_key = f"{event.interaction_id}:{event.action}:{event.review_id}"
    if event.idempotency_key != expected_key:
        return False, "invalid_feedback_idempotency_key"
    if not isinstance(event.event_id, str) or not event.event_id or _contains_secret_marker(event.event_id):
        return False, "unsafe_feedback_event"
    if not isinstance(event.timestamp, str) or any(ch in event.timestamp for ch in "\r\n\x00"):
        return False, "unsafe_feedback_event"
    return True, "ok"


def build_discord_feedback_event(
    *,
    payload: dict[str, Any],
    action: str,
    review_id: str,
    result: str,
) -> DiscordInteractionFeedbackEvent | None:
    """Create a sanitized MIM feedback event from a validated component."""

    interaction_id = payload.get("id")
    if not _safe_component(interaction_id) or not _safe_component(review_id):
        return None
    idempotency_key = f"{interaction_id}:{action}:{review_id}"
    event_hash = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:16]
    event = DiscordInteractionFeedbackEvent(
        event_id=f"discord-feedback-{event_hash}",
        idempotency_key=idempotency_key,
        timestamp=_utc_timestamp(),
        interaction_id=interaction_id,
        action=action,
        review_id=review_id,
        source=_FEEDBACK_SOURCE,
        endpoint_version=_FEEDBACK_ENDPOINT_VERSION,
        result=result,
    )
    ok, _code = validate_discord_feedback_event(event)
    return event if ok else None


def record_discord_feedback_event(
    *,
    sink: DiscordInteractionFeedbackSink | None,
    idempotency_keys: set[str] | None,
    payload: dict[str, Any],
) -> str:
    """Append a sanitized event after ACK construction.

    Missing sink means disabled local artifact capture. This is not an error for
    the Discord callback because ACK speed and safety are the first priority.
    """

    if sink is None:
        return "disabled"
    if payload.get("type") != 3:
        return "skipped"
    parsed = _parse_component(payload)
    if parsed is None:
        return "skipped"
    action, review_id = parsed
    interaction_id = payload.get("id")
    if not isinstance(interaction_id, str):
        return "skipped"
    idempotency_key = f"{interaction_id}:{action}:{review_id}"
    result = "duplicate" if idempotency_keys is not None and idempotency_key in idempotency_keys else "preview_ack"
    if idempotency_keys is not None:
        idempotency_keys.add(idempotency_key)
    event = build_discord_feedback_event(payload=payload, action=action, review_id=review_id, result=result)
    if event is None:
        return "invalid"
    try:
        return sink.record(event)
    except Exception:
        return "sink_error"


def validate_discord_work_item(item: Any) -> tuple[bool, str]:
    """Validate local queue rows before a future worker can consume them."""

    if not isinstance(item, DiscordInteractionWorkItem):
        return False, "invalid_work_item"
    if item.kind != _WORK_ITEM_KIND:
        return False, "invalid_work_item_kind"
    if item.source != _FEEDBACK_SOURCE or item.endpoint_version != _FEEDBACK_ENDPOINT_VERSION:
        return False, "invalid_work_item_source"
    if item.signature or item.headers:
        return False, "unsafe_work_item"
    if item.runtime_write or item.live_send or item.shell or item.env_lookup:
        return False, "unsafe_work_item"
    if item.action not in {"approve", "reject", "defer"}:
        return False, "invalid_work_item_action"
    if item.status not in _WORK_ITEM_STATUSES:
        return False, "invalid_work_item_status"
    if not _safe_component(item.interaction_id) or not _safe_component(item.review_id):
        return False, "unsafe_work_item"
    expected_key = f"{item.interaction_id}:{item.action}:{item.review_id}"
    if item.idempotency_key != expected_key:
        return False, "invalid_work_item_idempotency_key"
    if not isinstance(item.work_id, str) or not item.work_id or _contains_secret_marker(item.work_id):
        return False, "unsafe_work_item"
    if not isinstance(item.timestamp, str) or any(ch in item.timestamp for ch in "\r\n\x00"):
        return False, "unsafe_work_item"
    return True, "ok"


def build_discord_work_item(
    *,
    payload: dict[str, Any],
    action: str,
    review_id: str,
    status: str,
) -> DiscordInteractionWorkItem | None:
    """Create a sanitized queue item for later processing outside the route."""

    interaction_id = payload.get("id")
    if not _safe_component(interaction_id) or not _safe_component(review_id):
        return None
    idempotency_key = f"{interaction_id}:{action}:{review_id}"
    work_hash = hashlib.sha256(("work:" + idempotency_key).encode("utf-8")).hexdigest()[:16]
    item = DiscordInteractionWorkItem(
        work_id=f"discord-work-{work_hash}",
        idempotency_key=idempotency_key,
        timestamp=_utc_timestamp(),
        interaction_id=interaction_id,
        action=action,
        review_id=review_id,
        source=_FEEDBACK_SOURCE,
        endpoint_version=_FEEDBACK_ENDPOINT_VERSION,
        status=status,
    )
    ok, _code = validate_discord_work_item(item)
    return item if ok else None


def record_discord_work_item(
    *,
    sink: DiscordInteractionWorkQueueSink | None,
    idempotency_keys: set[str] | None,
    payload: dict[str, Any],
) -> str:
    """Append a queue item for later workers without running the work inline."""

    if sink is None:
        return "disabled"
    if payload.get("type") != 3:
        return "skipped"
    parsed = _parse_component(payload)
    if parsed is None:
        return "skipped"
    action, review_id = parsed
    interaction_id = payload.get("id")
    if not isinstance(interaction_id, str):
        return "skipped"
    idempotency_key = f"{interaction_id}:{action}:{review_id}"
    status = "duplicate" if idempotency_keys is not None and idempotency_key in idempotency_keys else "queued"
    if idempotency_keys is not None:
        idempotency_keys.add(idempotency_key)
    item = build_discord_work_item(payload=payload, action=action, review_id=review_id, status=status)
    if item is None:
        return "invalid"
    try:
        return sink.enqueue(item)
    except Exception:
        return "queue_error"


def _row_to_work_item(row: dict[str, Any]) -> DiscordInteractionWorkItem | None:
    allowed = {
        "work_id",
        "idempotency_key",
        "timestamp",
        "interaction_id",
        "action",
        "review_id",
        "source",
        "endpoint_version",
        "status",
        "kind",
        "runtime_write",
        "live_send",
        "shell",
        "env_lookup",
        "signature",
        "headers",
    }
    try:
        item = DiscordInteractionWorkItem(**{key: value for key, value in row.items() if key in allowed})
    except TypeError:
        return None
    ok, _code = validate_discord_work_item(item)
    return item if ok else None


def inspect_discord_work_queue(path: Path | str) -> DiscordInteractionWorkQueueSummary:
    """Read and summarize the local queue without applying anything."""

    queue_path = Path(path).expanduser()
    if not queue_path.exists():
        return DiscordInteractionWorkQueueSummary(path=str(queue_path), exists=False)

    total_rows = 0
    invalid_rows = 0
    valid_items: list[DiscordInteractionWorkItem] = []
    with queue_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if not line.strip():
                continue
            total_rows += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                invalid_rows += 1
                continue
            if not isinstance(row, dict):
                invalid_rows += 1
                continue
            item = _row_to_work_item(row)
            if item is None:
                invalid_rows += 1
                continue
            valid_items.append(item)

    action_counts = Counter(item.action for item in valid_items)
    review_counts = Counter(_redacted_label("review", item.review_id) for item in valid_items)
    latest = max((item.timestamp for item in valid_items), default=None)
    candidates = _work_queue_candidates(valid_items)
    return DiscordInteractionWorkQueueSummary(
        path=str(queue_path),
        exists=True,
        total_rows=total_rows,
        valid_items=len(valid_items),
        invalid_rows=invalid_rows,
        queued=sum(1 for item in valid_items if item.status == "queued"),
        duplicates=sum(1 for item in valid_items if item.status == "duplicate"),
        action_counts=dict(action_counts),
        review_counts=dict(review_counts),
        latest_timestamp=latest,
        candidates=candidates,
    )


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "없음"
    emojis = {"approve": "✅", "reject": "🛑", "defer": "🕊️"}
    return ", ".join(f"{emojis.get(key, '•')} {key}: {value}" for key, value in sorted(counts.items()))


def _redacted_label(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}#{digest}"


def _parse_queue_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _has_duplicate_burst(items: list[DiscordInteractionWorkItem], *, threshold: int = 5, minutes: int = 30) -> bool:
    duplicate_times = sorted(
        parsed
        for item in items
        if item.status == "duplicate" and (parsed := _parse_queue_timestamp(item.timestamp)) is not None
    )
    if len(duplicate_times) < threshold:
        return False
    window = timedelta(minutes=minutes)
    for idx, start in enumerate(duplicate_times):
        end_idx = idx + threshold - 1
        if end_idx < len(duplicate_times) and duplicate_times[end_idx] - start <= window:
            return True
    return False


def _state_preview(action_counts: Counter[str]) -> str:
    if len(action_counts) != 1:
        return "mixed"
    action = next(iter(action_counts))
    return {"approve": "accepted", "reject": "rejected", "defer": "needs_review"}.get(action, "needs_review")


def _work_queue_candidates(items: list[DiscordInteractionWorkItem]) -> list[DiscordInteractionWorkQueueCandidate]:
    by_review: dict[str, list[DiscordInteractionWorkItem]] = {}
    for item in items:
        by_review.setdefault(item.review_id, []).append(item)

    candidates: list[DiscordInteractionWorkQueueCandidate] = []
    for review_id in sorted(by_review):
        group = by_review[review_id]
        action_counts = Counter(item.action for item in group)
        state = _state_preview(action_counts)
        weight = action_counts.get("approve", 0) - action_counts.get("reject", 0)
        warnings: list[str] = []
        if len(action_counts) > 1:
            warnings.append("conflicting_actions")
        if _has_duplicate_burst(group):
            warnings.append("excessive_duplicates")
        apply_candidate = "review_needed" if warnings else state
        candidates.append(
            DiscordInteractionWorkQueueCandidate(
                review_label=_redacted_label("review", review_id),
                action_counts=dict(action_counts),
                queued=sum(1 for item in group if item.status == "queued"),
                duplicates=sum(1 for item in group if item.status == "duplicate"),
                state_preview=state,
                weight_preview=weight,
                apply_candidate=apply_candidate,
                warnings=tuple(warnings),
                latest_timestamp=max((item.timestamp for item in group), default=None),
            )
        )
    return candidates


def render_discord_work_queue_digest_ko(summary: DiscordInteractionWorkQueueSummary, *, verbose: bool = False) -> str:
    """Render a Korean read-only digest without raw JSON/log exposure."""

    if not summary.exists or summary.valid_items == 0:
        return "\n".join(
            [
                "📬 Discord 버튼 queue 요약",
                "- 아직 유효한 버튼 queue가 없습니다.",
                "- 실제 적용은 아직 하지 않았습니다.",
                "- 🛡️ DB write 없음.",
                "- 📴 live send 없음.",
                "- 🤖 agent 실행 없음.",
            ]
        )
    lines = [
        "📬 Discord 버튼 queue 요약",
        f"- ✅ 총 유효 항목: {summary.valid_items}",
        f"- ⏳ 대기: {summary.queued}",
        f"- 🔁 중복: {summary.duplicates}",
        f"- 🎛️ action: {_format_counts(summary.action_counts)}",
        f"- 🧾 review: {_format_counts(summary.review_counts)}",
        f"- 🕒 최신 시각: {summary.latest_timestamp or '없음'}",
        f"- ⚠️ 무효/스킵 행: {summary.invalid_rows}",
        "- 실제 적용은 아직 하지 않았습니다.",
        "- DB write 없음. live send 없음. agent 실행 없음.",
    ]
    if verbose and summary.candidates:
        lines.append("후보 preview")
        for candidate in summary.candidates:
            warnings = ",".join(candidate.warnings) if candidate.warnings else "없음"
            lines.append(
                "- "
                f"{candidate.review_label}: "
                f"actions={_format_counts(candidate.action_counts)}, "
                f"state_preview={candidate.state_preview}, "
                f"weight_preview={candidate.weight_preview}, "
                f"apply_candidate={candidate.apply_candidate}, "
                f"warnings={warnings}"
            )
    return "\n".join(lines)


def _safe_route_path(value: Any) -> str:
    path = str(value or DISCORD_INTERACTION_ROUTE).strip()
    if not path.startswith("/") or "//" in path or any(ch in path for ch in "\r\n\x00"):
        return DISCORD_INTERACTION_ROUTE
    return path


def resolve_discord_interaction_config(extra: dict[str, Any] | None) -> DiscordInteractionConfig:
    """Resolve the API-server Discord interaction route config.

    The resolver deliberately ignores token/secret/webhook-looking env names.
    Discord interaction verification needs the application public key, not a
    bot token, client secret, webhook URL, or Telegram credential.
    """

    section = (extra or {}).get("discord_interactions") or {}
    if not isinstance(section, dict) or section.get("enabled") is not True:
        return DiscordInteractionConfig()

    route_path = _safe_route_path(section.get("route") or section.get("route_path"))
    engine_name = str(section.get("engine") or section.get("engine_name") or "preview").strip().lower()
    if engine_name not in _ALLOWED_ENGINE_NAMES:
        return DiscordInteractionConfig(route_path=route_path)

    public_key = str(section.get("public_key") or "").strip()
    public_key_env = str(section.get("public_key_env") or "").strip()
    if not public_key and public_key_env:
        if public_key_env not in _ALLOWED_PUBLIC_KEY_ENV_NAMES or _looks_like_secret_env(public_key_env):
            return DiscordInteractionConfig(route_path=route_path)
        public_key = os.getenv(public_key_env, "").strip()

    if not public_key:
        return DiscordInteractionConfig(route_path=route_path)

    return DiscordInteractionConfig(enabled=True, public_key=public_key, route_path=route_path, engine_name=engine_name)


def validate_discord_interaction_timestamp(
    timestamp: str,
    *,
    now: float | None = None,
    max_age_seconds: int = DISCORD_TIMESTAMP_MAX_AGE_SECONDS,
    max_future_skew_seconds: int = DISCORD_TIMESTAMP_MAX_FUTURE_SKEW_SECONDS,
) -> tuple[bool, str]:
    """Validate Discord's timestamp before signature and JSON handling.

    Discord signs `timestamp + raw_body`. A good signature can still be replayed
    shortly after capture, so stale and far-future timestamps fail before any
    dry-run handler can run. The parser accepts ASCII decimal epoch seconds only
    to avoid Unicode digit or formatting surprises.
    """

    if not isinstance(timestamp, str) or not _ASCII_DECIMAL_RE.fullmatch(timestamp):
        return False, "invalid_timestamp"
    request_time = int(timestamp)
    current = time.time() if now is None else float(now)
    if request_time > current + max_future_skew_seconds:
        return False, "future_timestamp"
    if current - request_time > max_age_seconds:
        return False, "stale_timestamp"
    return True, "ok"


def default_discord_signature_verifier(*, public_key: str, timestamp: str, signature: str, body: bytes) -> bool:
    """Verify Discord's Ed25519 signature over `timestamp + raw_body`.

    PyNaCl is already present in Hermes' locked dependency graph for Discord
    voice support. If it is unavailable in a minimal runtime, the verifier fails
    closed instead of accepting unsigned interaction callbacks.
    """

    try:
        signing = importlib.import_module("nacl.signing")
        exceptions = importlib.import_module("nacl.exceptions")
        verify_key = signing.VerifyKey(bytes.fromhex(public_key))
        verify_key.verify(timestamp.encode("ascii") + body, bytes.fromhex(signature))
        return True
    except Exception:
        return False


def validate_discord_dry_run_result(result: Any) -> tuple[bool, str]:
    """Reject handler results that claim live or runtime side effects.

    The interaction route is still a preview path. Even an injected handler must
    not smuggle an "applied", "runtime_write", or "live_send" success claim into
    the ACK response during the local safety gate.
    """

    if not isinstance(result, dict):
        return True, "ok"
    for field_name in ("live_send", "runtime_write", "db_write", "applied", "apply"):
        if result.get(field_name) is True:
            return False, "unsafe_dry_run_result"
    return True, "ok"


def validate_discord_engine_result(result: Any) -> tuple[bool, str]:
    """Reject local engine results that claim side effects."""

    if not isinstance(result, DiscordInteractionEngineResult):
        return False, "unsafe_engine_result"
    if result.live_send or result.runtime_write or result.applied:
        return False, "unsafe_engine_result"
    return True, "ok"


def _current_time_from(now: Callable[[], float] | float | None) -> float | None:
    """Resolve an injected clock while keeping static type checkers happy."""

    if now is None:
        return None
    if isinstance(now, (int, float)):
        return float(now)
    return float(cast(Callable[[], float], now)())


def _error_response(code: str, status: int):
    return _aiohttp_web().json_response({"error": code}, status=status)


def _parse_component(payload: dict[str, Any]) -> tuple[str, str] | None:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    custom_id = data.get("custom_id")
    if not isinstance(custom_id, str) or len(custom_id) > 180:
        return None
    match = _CUSTOM_ID_RE.match(custom_id)
    if not match:
        return None
    return match.group(1), match.group(2)


def _contains_secret_marker(value: str) -> bool:
    return _looks_like_secret_env(value)


_ACTION_ACK_COPY = {
    "approve": {"emoji": "✅", "label": "승인", "tone": "승인으로 기록했어요"},
    "reject": {"emoji": "🛑", "label": "거부", "tone": "거부로 기록했어요"},
    "defer": {"emoji": "🕊️", "label": "보류", "tone": "보류로 기록했어요"},
}


def _safe_button_label(value: Any) -> str:
    if not isinstance(value, str):
        return "선택지"
    cleaned = " ".join(value.replace("\r", " ").replace("\n", " ").replace("\x00", " ").split())
    if not cleaned or _contains_secret_marker(cleaned):
        return "선택지"
    return cleaned[:80]


def _disabled_message_components(payload: dict[str, Any], *, selected_custom_id: str, action: str) -> list[dict[str, Any]]:
    """Copy the original component rows but make every button inert.

    Discord message-update ACKs need components in the response if the visible
    buttons should change immediately. We preserve Discord's component shape, but
    only copy the small allowlisted fields needed to render disabled buttons.
    """

    message = payload.get("message")
    if not isinstance(message, dict):
        return []
    rows = message.get("components")
    if not isinstance(rows, list):
        return []
    safe_rows: list[dict[str, Any]] = []
    action_copy = _ACTION_ACK_COPY.get(action, _ACTION_ACK_COPY["defer"])
    for row in rows[:5]:
        if not isinstance(row, dict) or row.get("type") != 1:
            continue
        out_row: dict[str, Any] = {"type": 1, "components": []}
        components = row.get("components")
        if not isinstance(components, list):
            continue
        for component in components[:5]:
            if not isinstance(component, dict) or component.get("type") != 2:
                continue
            safe_component: dict[str, Any] = {
                "type": 2,
                "style": component.get("style") if type(component.get("style")) is int else 2,
                "label": _safe_button_label(component.get("label")),
                "disabled": True,
            }
            custom_id = component.get("custom_id")
            if isinstance(custom_id, str) and len(custom_id) <= 180 and "\n" not in custom_id and "\r" not in custom_id:
                safe_component["custom_id"] = custom_id
                if custom_id == selected_custom_id:
                    safe_component["label"] = f"{action_copy['emoji']} 선택됨 · {action_copy['label']}"
            out_row["components"].append(safe_component)
        if out_row["components"]:
            safe_rows.append(out_row)
    return safe_rows


def _decorated_decision_ack_content(action: str) -> str:
    copy = _ACTION_ACK_COPY.get(action, _ACTION_ACK_COPY["defer"])
    return "\n".join(
        [
            f"{copy['emoji']} {copy['tone']} ({action})",
            "",
            "🌱 다음 단계",
            "- append-only queue에 안전하게 남길 준비를 했습니다. (queued)",
            "- 실제 업데이트는 아직 실행하지 않았습니다.",
            "",
            "🛡️ 안전 경계",
            "- DB write 없음",
            "- 도구 설치/삭제 없음",
            "- agent subprocess 실행 없음",
        ]
    )


def build_discord_runner_input(payload: dict[str, Any]) -> DiscordInteractionRunnerInput | None:
    """Build the narrow preview-runner input from a validated component payload."""

    if payload.get("type") != 3:
        return None
    parsed = _parse_component(payload)
    if parsed is None:
        return None
    interaction_id = payload.get("id")
    if not isinstance(interaction_id, str) or len(interaction_id) > 128 or any(ch in interaction_id for ch in "\r\n\x00"):
        return None
    action, review_id = parsed
    return DiscordInteractionRunnerInput(
        action=action,
        review_id=review_id,
        interaction_type=3,
        interaction_id=interaction_id,
    )


def runner_result_to_engine_result(result: Any, *, engine_name: str) -> DiscordInteractionEngineResult:
    """Convert a typed runner proposal into a Discord ACK or fail closed."""

    if not isinstance(result, DiscordInteractionRunnerResult):
        return DiscordInteractionEngineResult(ack=None, engine_name=engine_name, runtime_write=True)
    if result.live_send or result.runtime_write or result.shell or result.env_lookup:
        return DiscordInteractionEngineResult(ack=None, engine_name=engine_name, runtime_write=True)
    if type(result.response_type) is not int or result.response_type != 4:
        return DiscordInteractionEngineResult(ack=None, engine_name=engine_name, runtime_write=True)
    if type(result.flags) is not int or result.flags != 64:
        return DiscordInteractionEngineResult(ack=None, engine_name=engine_name, runtime_write=True)
    if type(result.content) is not str or not result.content or len(result.content) > _RUNNER_CONTENT_MAX_LENGTH:
        return DiscordInteractionEngineResult(ack=None, engine_name=engine_name, runtime_write=True)
    if any(ch in result.content for ch in "\r\n\x00") or _contains_secret_marker(result.content):
        return DiscordInteractionEngineResult(ack=None, engine_name=engine_name, runtime_write=True)
    return DiscordInteractionEngineResult(
        engine_name=engine_name,
        ack={"type": result.response_type, "data": {"flags": result.flags, "content": result.content}},
    )


def build_discord_interaction_engine(
    engine_name: str,
    *,
    runner: DiscordInteractionPreviewRunner | None = None,
) -> DiscordInteractionEngine:
    """Build an allowlisted local interaction engine.

    This is a registry seam, not a plugin loader. Config cannot name Python
    import paths, shell commands, or external agents. Future agent-specific
    wrappers must be wired as trusted objects by gateway code after review.
    """

    normalized = str(engine_name or "preview").strip().lower()
    if normalized not in _ALLOWED_ENGINE_NAMES:
        raise ValueError("unsupported_discord_interaction_engine")
    return PreviewDiscordInteractionEngine(name=normalized, runner=runner)


def build_discord_ack_preview(payload: dict[str, Any], dry_run_result: Any = None) -> dict[str, Any] | None:
    """Build a Discord interaction ACK preview after signature verification.

    Type 1 is Discord PING and must return PONG (`{"type": 1}`). Type 3 is a
    component interaction; this helper returns an ephemeral queue message. It
    never applies approval decisions, writes runtime state, or echoes handler
    output back to Discord.
    """

    _ = dry_run_result
    if payload.get("type") == 1:
        return {"type": 1}

    if payload.get("type") != 3:
        return None

    parsed = _parse_component(payload)
    if parsed is None:
        return None
    action, _review_id = parsed
    selected_custom_id = payload.get("data", {}).get("custom_id") if isinstance(payload.get("data"), dict) else ""
    components = _disabled_message_components(payload, selected_custom_id=selected_custom_id, action=action)
    if components:
        return {
            "type": 7,
            "data": {
                "content": _decorated_decision_ack_content(action),
                "components": components,
            },
        }
    return {
        "type": 4,
        "data": {
            "flags": 64,
            "content": _decorated_decision_ack_content(action),
        },
    }


async def handle_discord_interaction_request(
    request,
    *,
    config: DiscordInteractionConfig,
    verifier: Callable[..., bool] | None = None,
    dry_run_handler: Callable[[dict[str, Any]], Any] | None = None,
    replay_cache: DiscordInteractionReplayCache | None = None,
    feedback_sink: DiscordInteractionFeedbackSink | None = None,
    feedback_idempotency_keys: set[str] | None = None,
    work_queue_sink: DiscordInteractionWorkQueueSink | None = None,
    work_queue_idempotency_keys: set[str] | None = None,
    engine: DiscordInteractionEngine | None = None,
    now: Callable[[], float] | float | None = None,
):
    """Handle a Discord interaction callback in local/dry-run mode.

    Safety order:
    1. Read raw bytes.
    2. Validate timestamp freshness.
    3. Verify signature headers against the raw body.
    4. Reject replays.
    5. Only then parse JSON and build ACK previews.
    """

    if not config.enabled or not config.public_key:
        return _error_response("discord_interactions_disabled", 404)

    body = await request.read()
    timestamp = request.headers.get(DISCORD_TIMESTAMP_HEADER, "")
    signature = request.headers.get(DISCORD_SIGNATURE_HEADER, "")
    if not timestamp or not signature:
        return _error_response("missing_signature", 401)

    current_time = _current_time_from(now)
    timestamp_ok, timestamp_code = validate_discord_interaction_timestamp(timestamp, now=current_time)
    if not timestamp_ok:
        return _error_response(timestamp_code, 401)

    verify = verifier or default_discord_signature_verifier
    if not verify(public_key=config.public_key, timestamp=timestamp, signature=signature, body=body):
        return _error_response("invalid_signature", 401)

    if replay_cache is None:
        return _error_response("replay_cache_required", 500)

    if replay_cache is not None:
        replay_ok, replay_code = replay_cache.check_and_remember(
            timestamp=timestamp,
            signature=signature,
            body=body,
            now=current_time,
        )
        if not replay_ok:
            return _error_response(replay_code, 409)

    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error_response("invalid_json", 400)
    if not isinstance(payload, dict):
        return _error_response("invalid_payload", 400)

    if payload.get("type") == 3 and _parse_component(payload) is None:
        return _error_response("invalid_component", 400)

    # Keep this route preview-only. It deliberately does not invoke arbitrary
    # dry-run/apply handlers; even "dry-run" callables can have side effects
    # before returning. A later live apply gate must introduce a side-effect-safe
    # executor contract under separate review.
    _ = dry_run_handler
    selected_engine = engine or PreviewDiscordInteractionEngine()
    engine_result = selected_engine.build_ack(payload)
    engine_ok, engine_code = validate_discord_engine_result(engine_result)
    if not engine_ok:
        return _error_response(engine_code, 500)

    ack = engine_result.ack
    if ack is None:
        return _error_response("unsupported_interaction", 400)
    record_discord_feedback_event(
        sink=feedback_sink,
        idempotency_keys=feedback_idempotency_keys,
        payload=payload,
    )
    record_discord_work_item(
        sink=work_queue_sink,
        idempotency_keys=work_queue_idempotency_keys,
        payload=payload,
    )
    return _aiohttp_web().json_response(ack)
