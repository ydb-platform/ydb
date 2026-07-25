# Agent instructions — performance duty investigator

Toolkit: `.github/scripts/utils/perfomance_tests_status/duty_agent`

Investigates a **frozen incident pack** exported from OLAP or TPC-C Now reports
(`Save context` / `Copy context` in dive). Works for **both** report kinds.

## CLI

```bash
# From repo root (or any cwd)
python3 .github/scripts/utils/perfomance_tests_status/duty_agent/run.py \
  --context /path/to/perf-duty-….json \
  --out /tmp/duty-card.md

# Skip network (sandbox / gh)
python3 …/duty_agent/run.py --context pack.json --out card.md --offline

# Optional GitHub issue search by fingerprint
python3 …/duty_agent/run.py --context pack.json --out card.md --gh
```

Exit codes: `0` ok, `2` bad/missing context, `1` unexpected error.

## Context schema

See [`schema.md`](schema.md). Required:

- `schema`: `"perf-duty-context/v1"`
- `report.kind`: `"olap"` | `"tpcc"`
- `selection.branch`, `selection.db`, `selection.suite`

Logs are **not** in the pack — only URLs. Harness fetches sandbox when
`selection.focus_run.report` is set (typical for OLAP).

## What the harness does

1. Validate context (olap / tpcc).
2. Rule-based pre-label from reasons / query error classes / sandbox text.
3. Fetch sandbox HTML (if URL) → error fingerprints + quotes.
4. Scan compact `suite_history` / sticky query history for first-seen signals.
5. Optional `gh search issues` on fingerprint (narrow query).
6. Write duty card markdown (+ optional JSON next to it with `--json`).

## Rules for the agent / human

**Do**

- Treat context as source of truth for *which* suite/run/query.
- Prefer infra fingerprints (`disconnected node`, sandbox OOM, timeout storms)
  over inventing product regressions.
- Keep next steps actionable (mute? reopen? cluster event? product ticket?).

**Do not**

- Mute tests without a human.
- Clone/build the whole ydb tree “just to look”.
- Put secrets / SA keys into context JSON or duty cards.

## Tests

```bash
cd .github/scripts/utils/perfomance_tests_status/duty_agent
python3 test_duty_agent.py
```

## Export from reports

- OLAP / TPC-C dive toolbar: **Save context** (download) / **Copy context**.
- Pack includes current Last-runs / compare focus, sticky query (OLAP),
  compact history, report URL, datalens link (TPC-C).
