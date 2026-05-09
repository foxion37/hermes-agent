"""Obsidian decision-note writer for Discord livegate feedback.

This module has two paths:

* temp/dry-run writers for safe previews;
* an explicit live-vault promotion writer for the already-approved canonical
  decision note.

Both paths keep the Discord callback thin. They never write runtime DBs, send
messages, run agents, or include raw Discord payload/secret material.
"""

from __future__ import annotations

import re
from pathlib import Path

from gateway.discord_interactions import DiscordInteractionWorkQueueSummary, render_discord_work_queue_digest_ko

DEFAULT_LIVE_Q_VAULT = Path.home() / "Library/Mobile Documents/iCloud~md~obsidian/Documents/Q"
_PROJECT_DECISION_DIR = Path("30_PROJECTS/Mount-Improbable/MEMORY/decisions")
_SAFE_COMPONENT_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,80}")
_SAFE_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _resolve(path: Path | str) -> Path:
    return Path(path).expanduser().resolve()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _validate_component(value: str, *, label: str) -> str:
    text = str(value or "").strip()
    pattern = _SAFE_DATE_RE if label == "created" else _SAFE_COMPONENT_RE
    if not pattern.fullmatch(text):
        raise ValueError(f"{label} must be a safe path component")
    return text


def _reject_live_q_vault(vault_root: Path) -> None:
    live = _resolve(DEFAULT_LIVE_Q_VAULT)
    resolved = _resolve(vault_root)
    if resolved == live or _is_relative_to(resolved, live):
        raise ValueError("live Q vault writes are disabled for this dry-run writer")


def _decision_note_path(vault_root: Path | str, *, created: str, slug: str) -> Path:
    created = _validate_component(created, label="created")
    slug = _validate_component(slug, label="slug")
    root = _resolve(vault_root)
    target_dir = (root / _PROJECT_DECISION_DIR).resolve()
    if not _is_relative_to(target_dir, root):
        raise ValueError("decision path escaped vault root")
    return target_dir / f"{created}-{slug}.md"


def render_discord_decision_note(
    summary: DiscordInteractionWorkQueueSummary,
    *,
    created: str,
    slug: str,
    status: str = "dry-run",
) -> str:
    """Render sanitized Korean Markdown for Obsidian decision-note promotion."""

    created = _validate_component(created, label="created")
    slug = _validate_component(slug, label="slug")
    status = _validate_component(status, label="status")
    is_canonical = status == "canonical"
    digest = render_discord_work_queue_digest_ko(summary, verbose=True)
    summary_line = (
        "- Discord 버튼 피드백 적용 정책을 live Q vault에 canonical decision으로 승격한 노트입니다."
        if is_canonical
        else "- Discord 버튼 피드백을 Obsidian-first 방식으로 승격하기 위한 dry-run 결정 노트입니다."
    )
    promotion_line = "- live Q vault write 완료." if is_canonical else "- 이 파일은 live Q vault promotion 전 preview입니다."
    next_lines = (
        [
            "- live Q vault write 완료.",
            "- 다음 단계는 callback 밖 실제 루프/자기개선형 MIM worker 설계입니다.",
            "- runtime DB write 없음.",
            "- live send 없음.",
            "- agent 실행 없음.",
        ]
        if is_canonical
        else [
            "- live Q vault write는 아직 하지 않습니다.",
            "- temp-vault TDD와 dry-run artifact 검수 후 별도 gate에서 promote합니다.",
            "- runtime DB write 없음.",
            "- live send 없음.",
            "- agent 실행 없음.",
            "- live Q vault write 없음.",
        ]
    )
    return "\n".join(
        [
            "---",
            f"id: {created}-{slug}",
            "type: decision",
            "area: discord-livegate",
            f"status: {status}",
            f"created: {created}",
            "source: discord_interaction_work_queue",
            "runtime_db_write: false",
            "live_send: false",
            "agent_execution: false",
            "---",
            "",
            "# Discord livegate feedback apply policy",
            "",
            "## 요약",
            "",
            summary_line,
            promotion_line,
            "- raw Discord payload and raw credential material are excluded.",
            "- raw interaction/work/idempotency/review IDs are excluded.",
            "",
            "## 판단",
            "",
            "- 정본 위치는 `MEMORY/decisions/`입니다.",
            "- `STATUS.md`에는 최신 운영 요약만 둡니다.",
            "- SQLite/runtime DB는 나중의 derived index/cache로 둡니다.",
            "- conflict 또는 duplicate burst는 apply 후보를 `review_needed`로 멈춥니다.",
            "",
            "## 현재 queue digest",
            "",
            digest,
            "",
            "## 다음 적용",
            "",
            *next_lines,
            "",
            "## 연결",
            "",
            "- STATUS.md",
            "- RESULT_SUMMARY.md",
        ]
    ) + "\n"


def write_discord_decision_note_to_temp_vault(
    vault_root: Path | str,
    summary: DiscordInteractionWorkQueueSummary,
    *,
    created: str,
    slug: str = "discord-livegate-feedback-policy",
) -> Path:
    """Write the note into a temp vault only; reject the live Q vault."""

    root = _resolve(vault_root)
    _reject_live_q_vault(root)
    note_path = _decision_note_path(root, created=created, slug=slug)
    if not _is_relative_to(note_path.resolve().parent, root):
        raise ValueError("decision note escaped vault root")
    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text(render_discord_decision_note(summary, created=created, slug=slug), encoding="utf-8")
    return note_path


def write_discord_decision_note_dry_run(
    artifact_root: Path | str,
    summary: DiscordInteractionWorkQueueSummary,
    *,
    created: str,
    slug: str = "discord-livegate-feedback-policy",
) -> Path:
    """Write a local dry-run artifact outside the live Obsidian vault."""

    created = _validate_component(created, label="created")
    slug = _validate_component(slug, label="slug")
    root = _resolve(artifact_root)
    live = _resolve(DEFAULT_LIVE_Q_VAULT)
    if root == live or _is_relative_to(root, live):
        raise ValueError("dry-run artifact root must not be inside the live Q vault")
    target_dir = (root / "discord-livegate-decision-notes").resolve()
    if not _is_relative_to(target_dir, root):
        raise ValueError("dry-run path escaped artifact root")
    target_dir.mkdir(parents=True, exist_ok=True)
    note_path = target_dir / f"{created}-{slug}.md"
    note_path.write_text(render_discord_decision_note(summary, created=created, slug=slug), encoding="utf-8")
    return note_path


def promote_discord_decision_note_to_live_vault(
    vault_root: Path | str,
    summary: DiscordInteractionWorkQueueSummary,
    *,
    created: str,
    slug: str = "discord-livegate-feedback-policy",
    overwrite: bool = False,
) -> Path:
    """Write the canonical decision note into the supplied Obsidian vault.

    This is an explicit promotion path, not callback code. It writes one Markdown
    note under ``MEMORY/decisions/`` and remains idempotent: an identical existing
    note is accepted; a different existing note is refused unless ``overwrite`` is
    set by an operator-facing CLI.
    """

    root = _resolve(vault_root)
    note_path = _decision_note_path(root, created=created, slug=slug)
    target_dir = note_path.parent.resolve()
    if not _is_relative_to(target_dir, root):
        raise ValueError("decision note escaped live vault root")
    content = render_discord_decision_note(summary, created=created, slug=slug, status="canonical")
    if note_path.exists():
        current = note_path.read_text(encoding="utf-8")
        if current == content:
            return note_path
        if not overwrite:
            raise FileExistsError("different decision note already exists")
    target_dir.mkdir(parents=True, exist_ok=True)
    note_path.write_text(content, encoding="utf-8")
    return note_path
