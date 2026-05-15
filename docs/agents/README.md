# Hermes Agent agent-docs index

This folder holds detailed guidance that was moved out of startup-loaded `AGENTS.md` to keep Codex/Hermes prompt surfaces small.

## Files

| File | Purpose |
|---|---|
| `hermes-development-guide.md` | Full developer guide extracted from the previous repo-local `AGENTS.md`. Read the relevant section when a task needs details. |

## Policy

- `../AGENTS.md` should stay a compact router with a normal target of 4–8KB; review before commit if it grows beyond 8KB.
- Detailed architecture, recipes, pitfalls, and examples belong here or in narrower docs.
- After changing startup guidance, run `codex debug prompt-input 'memory compact probe'` from the repo root and record before/after bytes when the change is part of prompt-surface work.
