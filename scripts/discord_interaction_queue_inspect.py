#!/usr/bin/env python3
"""Read-only digest for Discord interaction work queue artifacts.

This helper is intentionally local and side-effect-free. It reads the append-only
JSONL queue produced by the Discord interaction livegate and prints a Korean
summary. It does not apply decisions, write runtime DBs, send Discord messages,
or spawn agent subprocesses.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway.discord_interactions import (
    filter_discord_work_queue_for_operations,
    inspect_discord_work_queue,
    render_discord_work_queue_digest_ko,
)

DEFAULT_QUEUE_PATH = Path.home() / ".hermes" / "discord-interactions" / "work-queue.jsonl"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only Discord interaction work queue digest")
    parser.add_argument(
        "path",
        nargs="?",
        default=str(DEFAULT_QUEUE_PATH),
        help="work-queue JSONL path; default: ~/.hermes/discord-interactions/work-queue.jsonl",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="show per-review read-only candidate preview rows",
    )
    parser.add_argument(
        "--operations",
        action="store_true",
        help="show read-only operations preview for a later worker; never applies decisions",
    )
    parser.add_argument(
        "--now",
        default=None,
        help="override current time for deterministic preview/diagnostics",
    )
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=72,
        help="operations preview expiry window in hours; default: 72",
    )
    args = parser.parse_args(argv)
    path = Path(args.path).expanduser()
    if args.operations:
        print(filter_discord_work_queue_for_operations(path, now=args.now, max_age_hours=args.max_age_hours).digest)
        return 0
    summary = inspect_discord_work_queue(path)
    print(render_discord_work_queue_digest_ko(summary, verbose=args.verbose))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
