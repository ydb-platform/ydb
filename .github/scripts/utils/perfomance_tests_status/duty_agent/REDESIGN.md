# Duty agent redesign — plan

Status: **implemented** (Phases 1–4). Entry: `dutyctl.py` + this doc + `AGENTS.md`.  
Model: same split as `arc_import_duty` — **Python = facts + validate**, **agent = thinking**.  
Legacy autopilot (`root_cause` / `narrative` / one-shot `run.py`) removed.

---

## 1. Why rewrite

Current `duty_agent` is an autopilot: `run.py` → classifiers → fixed MD. That:

- encodes yesterday’s heuristics (`infra_mid_suite`, known/NEW/consequence, ≤40 lines);
- skips real loops (bisect / hypothesis check only after human pressure);
- pretends certainty when evidence is thin;
- does not fit OLAP **slow** or TPC-C **tpmc/lat** the same way as OLAP **fail**.

`arc_import_duty` works on hard cases because code never “guesses root cause” — it feeds evidence; the agent decides; `validate-report` rejects bullshit.

---

## 2. Goals / non-goals

### Goals

1. **One core** for all perf-duty packs (`perf-duty-context/v1`).
2. Agent **chooses per problem** what to investigate (tools + playbooks by analysis type).
3. Find, in plain language: **what broke → why → who → mechanism**; **verify** the hypothesis; **retry** if falsified.
4. If unclear → **`investigate_further` / `unknown`**, not a fake confident story.
5. Outputs: short human **`analysis.md`** + machine **`result.json`** (counts, confidence, culprit, errors).

### Non-goals

- Auto-mute / auto-ticket without human.
- One-shot Python that invents `root_cause.kind`.
- Forcing every incident into the same 6-section essay.
- Replacing Now UI / YDB QA marts (we consume context packs).

---

## 3. Architecture

```
                    ┌─────────────────────────┐
  context pack ──►  │  dutyctl (tools only)   │  fetch / contrast / bisect / metrics
                    └───────────┬─────────────┘
                                │ JSON artifacts in <run_dir>/
                                ▼
                    ┌─────────────────────────┐
                    │  Agent (AGENTS.md)      │  hypotheses, loops, prose
                    │  picks playbook by type │
                    └───────────┬─────────────┘
                                │ analysis.md draft
                                ▼
                    ┌─────────────────────────┐
                    │  dutyctl validate       │  structure + anti-bullshit
                    └───────────┬─────────────┘
                                ▼
                         analysis.md + result.json
```

| Layer | Owns | Must not |
|-------|------|----------|
| **dutyctl** (Python) | tokens, fetch Allure/attachments, prior scans, metric deltas, sha→PR helpers, code bisect window, validate, write `result.json` skeleton | decide culprit / final kind; issue search via `gh` |
| **AGENTS.md** | playbooks per analysis type, evidence bar, retry loop, tone of conclusion | become a novel |
| **Agent** | which tools to call next, per-problem plan, hypothesis verify, final prose | skip validate; invent owners without evidence |

`run.py` one-shot autopilot is **deprecated** (kept temporarily as `dutyctl investigate-legacy` if needed, then deleted).

---

## 4. Analysis types (one core, many playbooks)

Derived from context (not hardcoded in classifiers as the only path):

| `analysis_type` | When | Primary evidence |
|-----------------|------|------------------|
| `olap_fail` | `report.kind=olap` and fails / `suite_now` failing / query `kind=fail` | Allure statusMessage, `kikimr__stderr` / logs, VERIFY/abort |
| `olap_slow` | olap + slow / ydb regression / query `kind=slow` (no hard fail) | timings, plots, compare vs base wave, optional logs for stalls |
| `tpcc_tpmc` | `report.kind=tpcc` and tpmC regression dominates | suite_history tpmc, DataLens, sha window, cluster load hints |
| `tpcc_lat` | tpcc and latency / lat_capped dominates | lat90 history, capped flag, WH, DataLens |
| `mixed` | several signals at once | agent splits into **problems[]**, each with its own type |

Detection helper (code, best-effort only):

```text
dutyctl detect-type --context pack.json  →  { analysis_types: [...], problems_seed: [...] }
```

Agent may override/split after looking at data (“seed said olap_fail, but stderr empty and only slow queries → reclassify”).

---

## 5. Core loop (mandatory)

For **each** problem in the inventory:

```
1. State hypothesis H (one sentence)
2. Pick tools for this analysis_type (agent decides order)
3. Gather evidence
4. Verify H:
   - supported → write conclusion + confidence
   - falsified → new H, retry (max N, default 3) for this problem
5. If still unclear after N → status=unknown, next_step=exact ask, confidence low
   DO NOT fabricate owner / “infra” / “known” without evidence
```

Global budget: e.g. `max_problem_retries=3`, `max_tool_calls≈40` per run (CI/agent guard). Prefer depth on top problems over shallow scan of 20.

### Evidence bar (all types)

Before naming a **culprit** (PR / author / component owner):

1. Crash/metric path or symbol intersects PR files **or**
2. Explicit mechanism + bisect shows path change in window **or**
3. For metrics-only: clear first-bad sha + PR that can affect that metric path (KQP/columnshard/…), stated as **candidate** not owner if weak.

“PR of focus sha” alone is **never** enough (viewer/mute noise).

If cannot meet the bar → `culprit_found=false`, `status=unknown|investigate_further`.

---

## 6. Tool surface (`dutyctl`)

Minimal CLI. Artifacts under `--out-dir` / auto `run_id`.

| Command | Purpose |
|---------|---------|
| `init-token` | YAV → `SANDBOX_TOKEN` |
| `prepare` | detect-type + focus(+fatal) + priors + metrics (tpcc/slow) |
| `dig-runs` | Mart history (~35d+): TPC-C all run_types + peer clusters; OLAP related suites + peer DbAlias (same branch) |
| `dig-prs` | product PRs + hot areas in jump window |
| `bisect` | path window prev…first-fail + focus PR files |
| `validate` | lint `analysis.md` |
| `write-result` | merge `problems.json` → `result.json` |

Issue search / extra PR digs: agent uses `gh` directly.  
No `build_root_cause()`. No auto markdown novelist.

---

## 7. Report (`analysis.md`) — plain language

### Tone

- Short sentences, duty-facing Russian or English (match requester).
- No taxonomy theater (`consequence`, `infra_mid_suite`) in the user-facing conclusion unless useful as a tag in JSON.
- No triple repetition of the same 2005 story.

### Suggested shape (flexible; validate enforces minimum, not essay length)

```markdown
# Perf duty — {suite} @ {db} — {focus_label}

## Заключение
- **Итог:** <одна фраза: что случилось>
- **Решение:** update_known | open_ticket | wait_next_wave | investigate_further | no_action
- **Виновник:** @{login} / PR #N / unknown — <почему да или почему нет>
- **Уверенность:** high | medium | low

## Проблемы
### P1 — <короткое имя>
- Тип: olap_fail | olap_slow | tpcc_tpmc | tpcc_lat
- Что сломалось: …
- Почему / механика: …
- Кто (если есть): … + доказательство
- Гипотеза проверена: yes | no | partial — …
- Связанный issue: #N или нет

## Что дальше
1. …
```

Agent may add an Evidence appendix **folded** or keep quotes short. Length limit: prefer **clarity**, soft cap ~80 lines (not a hard 40 that forces lies).

### Forbidden (validate)

- Confident culprit without evidence bar.
- “NEW ticket” for a surface symptom of a stated parent abort without separate proof.
- Stopping at `code: 2005` / node lost for `olap_fail` without stderr dig or explicit “stderr empty”.
- Blaming focus-wave PR when `code-bisect` shows crash path unchanged (unless other proof).
- Empty заключение / only fingerprint list.

---

## 8. Machine status (`result.json`)

Schema: **`perf-duty-result/v1`**. Written every run (success or fail).

```json
{
  "schema": "perf-duty-result/v1",
  "ok": true,
  "run_id": "2026-07-25_f88e100_UploadTpch100",
  "context": {
    "kind": "olap",
    "branch": "main",
    "db": "sas_big_column",
    "suite": "UploadTpch100",
    "focus_sha": "f88e100",
    "focus_label": "2026-07-25_f88e100"
  },
  "analysis_types": ["olap_fail"],
  "status": "completed",
  "resolution": "update_known",
  "summary": "Columnshard Groups.end VERIFY; known #29944; not introduced in focus wave.",
  "confidence": "high",
  "confidence_score": 0.85,
  "culprit_found": false,
  "culprit": null,
  "problems": {
    "total": 2,
    "analyzed": 2,
    "unknown": 0,
    "items": [
      {
        "id": "P1",
        "analysis_type": "olap_fail",
        "title": "VERIFY Groups.end OnReadResult",
        "status": "analyzed",
        "resolution": "update_known",
        "confidence": "high",
        "culprit_found": false,
        "culprit": null,
        "issue": { "number": 29944, "url": "…" },
        "hypothesis_retries": 1,
        "verified": true,
        "notes": "bisect: read.cpp unchanged 13288b3…f88e100"
      }
    ]
  },
  "errors": [],
  "warnings": [],
  "artifacts": {
    "analysis_md": "analysis.md",
    "focus_json": "focus.json",
    "priors_json": "priors.json"
  },
  "timings_sec": { "total": 120 }
}
```

### Field rules

| Field | Meaning |
|-------|---------|
| `ok` | process finished without tool/infra hard failure |
| `status` | `completed` \| `partial` \| `failed` \| `stopped` |
| `resolution` | rollup of problems (worst/primary) |
| `culprit_found` | true only if evidence bar met |
| `problems.total` | seeded + discovered |
| `problems.analyzed` | reached a non-bullshit conclusion (incl. unknown) |
| `problems.unknown` | explicitly could not determine |
| `errors[]` | `{ "stage", "message", "retriable" }` — sandbox 401, gh rate limit, … |
| `warnings[]` | soft: sticky-green suite red, stderr missing, … |

If sandbox fetch dies: `ok=false`, `status=failed`, `errors` filled, `analysis.md` may still say what was attempted.

---

## 9. Playbooks (agent-owned; sketch)

### `olap_fail`

stderr → fatal → cluster problems → priors all fails → issue search (fatal tokens) → code-bisect on crash path → PR files ∩ path → verify (“is 2005 only consequence?”).

### `olap_slow`

metrics-delta (ydb / query times) → first-bad wave → bisect dirs that can affect planner/columnshard/KQP → logs only if stall/timeout suspected → no fake VERIFY story.

### `tpcc_tpmc` / `tpcc_lat`

`dig-runs` on `perfomance/tpcc` / olap (neighbors, ~35d; widen if edge) → largest step →
`dig-prs` on that window → metrics-delta + DataLens → hot PR flags on/off →
often **candidate** / `wait_next_wave` only after dig.
**Forbidden:** stop at pack `suite_history` only, «no Allure», or blame alert-commit PR without mart + interval dig.

Agent **decides** which subset of tools to run per problem; core does not force one global path.

---

## 10. What happens to current code

| Keep / evolve into dutyctl | Delete or demote |
|----------------------------|------------------|
| `tools/sandbox.py`, `attachments.py`, `http_fetch.py`, `yav.py` | `tools/root_cause.py` as truth |
| `tools/contrast.py` → `fetch-priors` | `tools/narrative.py` auto novelist |
| `tools/code_bisect.py` | `tools/classify.py` / `problems.py` status enums as final answer |
| `tools/github_pr.py` (sha→PR); issue search via `gh` | `run.py` one-shot default |
| `tools/context.py`, schema | rigid `REPORT_TEMPLATE` 6-H2 law |
| token_config / `tests/fixtures` | — |

`AGENTS.md` rewritten around loop + playbooks + evidence bar (this doc).  
`REDESIGN.md` (this file) = north star until implemented.

---

## 11. Phases

### Phase 0 — Contract (this doc) ✅

Agree architecture, `result.json`, analysis types, anti-bullshit rules.

### Phase 1 — dutyctl skeleton

- `dutyctl` entry + `init-token`, `detect-type`, `fetch-focus`, `fetch-priors`, `write-result`
- Run dir artifacts; no auto MD
- Stop using autopilot as default in AGENTS (“call tools; you write analysis.md”)

### Phase 2 — Agent loop + validate

- New `AGENTS.md` (outcomes, evidence bar, retries, plain-language заключение)
- `validate` forbidden patterns + required заключение fields
- `result.json` always emitted

### Phase 3 — Playbooks + metrics

- `metrics-delta` for olap_slow / tpcc_*
- `scan-fatal`, `code-bisect`, `issue-search` wired as subcommands
- Mixed packs: multiple problems, independent loops

### Phase 4 — Delete legacy

- Remove `build_root_cause` / narrative renderer / old card schema as primary
- Optional: thin `investigate-legacy` removed
- Tests: `tests/test_duty_agent.py` + `tests/fixtures/`; golden `analysis.md` only for validate fixtures

---

## 12. Success criteria

A redesign is “done” when:

1. On the UploadTpch100 / Groups.end pack, an agent using **only** dutyctl + AGENTS reaches #29944 + bisect unchanged **without** a human correcting taxonomy mid-flight.
2. A TPC-C lat/tpmc pack gets a non-VERIFY playbook and can end in `unknown` / candidate PR without inventing a crash.
3. `result.json` always present; on sandbox 401, `errors` non-empty and `ok=false`.
4. No Python module claims sole ownership of root cause.

---

## 13. How to run (current)

Minimal CLI: `prepare` | `bisect` | `validate` | `write-result` (+ `init-token`).

```bash
cd .github/scripts/utils/perfomance_tests_status/duty_agent
eval "$(python3 dutyctl.py init-token --shell)"
OUT=./runs/case1
python3 dutyctl.py prepare -c CONTEXT.json -o $OUT
python3 dutyctl.py bisect -c CONTEXT.json -o $OUT
# agent: write analysis.md + problems.json; optional gh search
python3 dutyctl.py validate -o $OUT
python3 dutyctl.py write-result -c CONTEXT.json -o $OUT --summary "…" --resolution update_known --confidence high
```
