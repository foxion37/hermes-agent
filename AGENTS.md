# AGENTS.md — Hermes Agent Repo Router

This file is the **thin startup router** for agents working in the Hermes Agent repository. Keep it compact. Detailed implementation guidance lives in `docs/agents/hermes-development-guide.md` and should be read only when relevant.

## Role

Work as a careful Hermes contributor: inspect the real files, keep diffs small, preserve unrelated changes, verify before reporting success, and never print secrets.

## Precedence

1. System/developer/user instructions.
2. This repo router (`AGENTS.md`).
3. Task-specific detail docs under `docs/agents/`.
4. Runtime evidence from the checkout, tests, logs, and config.

Direct user/session instructions override this router. Repo-local rules override workspace defaults only for Hermes repo build/test/risk details.

## Startup budget rule

`AGENTS.md` is an index, not a manual. Do not paste long architecture notes here. If guidance grows beyond a compact routing table, move it to `docs/agents/hermes-development-guide.md` or another focused doc and link it here.

## Development environment

Prefer the repo venv that exists:

```bash
source .venv/bin/activate  # preferred when present
# or
source venv/bin/activate
```

`scripts/run_tests.sh` probes `.venv`, then `venv`, then `$HOME/.hermes/hermes-agent/venv`.

## Load detailed guidance by task

| Task | Read this first |
|---|---|
| Overall repo map | `docs/agents/hermes-development-guide.md#project-structure` |
| Core agent loop / model calls | `docs/agents/hermes-development-guide.md#aiagent-class-run_agentpy` |
| CLI / slash commands / prompt_toolkit | `docs/agents/hermes-development-guide.md#cli-architecture-clipy` |
| TUI / Ink / native scrollback | `docs/agents/hermes-development-guide.md#tui-architecture-ui-tui-tui_gateway` |
| Add or modify tools | `docs/agents/hermes-development-guide.md#adding-new-tools` |
| Config schema/defaults | `docs/agents/hermes-development-guide.md#adding-configuration` |
| Plugin system | `docs/agents/hermes-development-guide.md#plugins` |
| Skill lifecycle / curator | `docs/agents/hermes-development-guide.md#skills` and `#curator-skill-lifecycle` |
| Delegation/subagents | `docs/agents/hermes-development-guide.md#delegation-delegate_task` |
| Cron scheduler | `docs/agents/hermes-development-guide.md#cron-scheduled-jobs` |
| Tests and commit conventions | `docs/agents/hermes-development-guide.md#testing` |

## Compact file map

| Path | Purpose |
|---|---|
| `run_agent.py` | `AIAgent`; core conversation loop, memory flush, compression, provider calls |
| `model_tools.py` | tool discovery and dispatch glue |
| `toolsets.py` | toolset definitions and core tool lists |
| `cli.py` | classic Hermes CLI orchestration and slash-command handling |
| `hermes_state.py` | SQLite session store and search |
| `hermes_constants.py` | profile-aware Hermes home helpers; use instead of hardcoding `~/.hermes` |
| `agent/` | provider adapters, prompt/context/memory/model routing internals |
| `hermes_cli/` | CLI subcommands, config, setup, commands, skin engine |
| `tools/` | built-in tool implementations and registry |
| `gateway/` | messaging gateway runtime and platform adapters |
| `cron/` | scheduler and job management |
| `ui-tui/` | Ink React terminal UI |
| `tui_gateway/` | Python JSON-RPC backend for TUI |
| `tests/` | pytest suite; focused tests first |
| `website/` | Docusaurus docs site |

## Common edit routes

### Add a tool

Primary paths: `tools/<name>.py`, `tools/registry.py`, `model_tools.py`, `toolsets.py`, and focused tests under `tests/`.

1. Create or modify `tools/<name>.py`.
2. Register through `tools/registry.py` using `registry.register(...)` with a `check_fn` and JSON-returning handler.
3. Ensure discovery/import routing in `model_tools.py` when needed.
4. Ensure toolset exposure in `toolsets.py` when needed.
5. Add focused tests.
6. Read `docs/agents/hermes-development-guide.md#adding-new-tools` before implementation.

### Add a slash command

Primary paths: `hermes_cli/commands.py`, `cli.py`, and optionally `gateway/run.py` for gateway handling.

1. Add `CommandDef` in `hermes_cli/commands.py`.
2. Add CLI handling in `cli.py::process_command()` or the relevant command path.
3. Add gateway handling in `gateway/run.py` only if needed.
4. Verify help/autocomplete consumers still derive correctly.
5. Read `docs/agents/hermes-development-guide.md#cli-architecture-clipy`.

### Change config behavior

1. Locate defaults and loaders in `hermes_cli/config.py` and related setup code.
2. Keep secrets in `.env`, not config docs or committed files.
3. Use profile-aware helpers where runtime paths are involved.
4. Read `docs/agents/hermes-development-guide.md#adding-configuration`.

### Gateway/API work

1. Keep live gateway restarts, endpoint registration, external sends, and runtime DB writes behind explicit Q approval.
2. Use local/focused tests first.
3. Never print tokens, webhook URLs, bot tokens, channel IDs, or API keys unless explicitly already public and safe.
4. For API auth, protected endpoint checks should use status codes, not secret values.

## Verification rules

- Start with the smallest focused test that covers the touched path.
- Use `python -m pytest ... -o 'addopts=' -q` when baked-in pytest flags interfere.
- Run broader related tests only after focused tests pass.
- Report unrelated/environment failures separately; do not weaken focused evidence.
- Before commit/push, run secret scanning such as `gitleaks protect --staged --redact` or `gitleaks detect --redact` when applicable.
- Do not commit, push, merge, restart services, mutate runtime DBs, install/remove packages, or perform live external actions without explicit Q approval.

## Prompt-surface measurement

After changing startup instructions, measure the prompt surface:

```bash
codex debug prompt-input 'memory compact probe' > /tmp/codex_prompt_probe.json
python3 - <<'PY'
import json
from pathlib import Path
p = Path('/tmp/codex_prompt_probe.json')
data = json.loads(p.read_text())
print('total', p.stat().st_size)
for i, m in enumerate(data):
    txt = '\n'.join(part.get('text','') for part in m.get('content',[]) if isinstance(part, dict))
    print(i, m.get('role'), len(txt.encode()))
PY
```

Target for this router: keep repo-local AGENTS/context small enough that Hermes repo prompt input stays materially below the previous 117,959-byte baseline.

## Safety reminders

- Preserve unrelated dirty changes.
- Prefer reversible docs/code edits.
- Do not hardcode `~/.hermes`; use `get_hermes_home()` where code needs runtime paths.
- Config values belong in `config.yaml`; secrets belong in `.env` or 1Password/runtime secret stores.
- New tools need requirement checks so unavailable tools do not appear unexpectedly.
- If detailed guidance is missing, update `docs/agents/hermes-development-guide.md`, not this router.
