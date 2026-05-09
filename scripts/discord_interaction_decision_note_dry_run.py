#!/usr/bin/env python3
"""Create a dry-run Obsidian decision note for Discord livegate feedback.

The default mode writes only under a local artifact directory. It never writes
Q's live Obsidian vault. Live promotion is a separate user-approved gate.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway.discord_interactions import inspect_discord_work_queue
from scripts.discord_interaction_decision_note import write_discord_decision_note_dry_run

DEFAULT_QUEUE_PATH = Path.home() / ".hermes" / "discord-interactions" / "work-queue.jsonl"
DEFAULT_ARTIFACT_ROOT = ROOT / ".hermes" / "dry-runs"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write a dry-run Obsidian decision note for Discord livegate feedback")
    parser.add_argument(
        "--queue-path",
        default=str(DEFAULT_QUEUE_PATH),
        help="work-queue JSONL path; default: ~/.hermes/discord-interactions/work-queue.jsonl",
    )
    parser.add_argument(
        "--artifact-root",
        default=str(DEFAULT_ARTIFACT_ROOT),
        help="dry-run artifact root; default: repo .hermes/dry-runs",
    )
    parser.add_argument("--created", default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD note date")
    parser.add_argument("--slug", default="discord-livegate-feedback-policy", help="safe note slug")
    args = parser.parse_args(argv)

    summary = inspect_discord_work_queue(Path(args.queue_path).expanduser())
    note_path = write_discord_decision_note_dry_run(
        Path(args.artifact_root).expanduser(),
        summary,
        created=args.created,
        slug=args.slug,
    )
    print(note_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
