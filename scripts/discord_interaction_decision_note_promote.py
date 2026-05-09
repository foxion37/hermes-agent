#!/usr/bin/env python3
"""Promote Discord livegate feedback policy into Obsidian canon.

This script is callback-outside only. It reads the local work queue, renders a
sanitized Korean decision note, and writes exactly one Markdown file under the
supplied Obsidian vault's MEMORY/decisions directory. It does not write runtime
DBs, send live messages, or execute agents.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway.discord_interactions import inspect_discord_work_queue
from scripts.discord_interaction_decision_note import DEFAULT_LIVE_Q_VAULT, promote_discord_decision_note_to_live_vault


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote Discord livegate decision note into Obsidian canon.")
    parser.add_argument(
        "--queue-path",
        default=str(Path.home() / ".hermes/discord-interactions/work-queue.jsonl"),
        help="Append-only Discord interaction work queue JSONL path.",
    )
    parser.add_argument(
        "--vault-root",
        default=str(DEFAULT_LIVE_Q_VAULT),
        help="Obsidian Q vault root. The note is written under 30_PROJECTS/Mount-Improbable/MEMORY/decisions/.",
    )
    parser.add_argument("--created", default=datetime.now(UTC).date().isoformat(), help="Decision date, YYYY-MM-DD.")
    parser.add_argument("--slug", default="discord-livegate-feedback-policy", help="Safe note slug.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing different note. Default refuses to overwrite.",
    )
    args = parser.parse_args()

    summary = inspect_discord_work_queue(Path(args.queue_path))
    note_path = promote_discord_decision_note_to_live_vault(
        Path(args.vault_root),
        summary,
        created=args.created,
        slug=args.slug,
        overwrite=args.overwrite,
    )
    print(note_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
