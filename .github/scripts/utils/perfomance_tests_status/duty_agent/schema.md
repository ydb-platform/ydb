# Schemas

## perf-duty-context/v1 (input)

Frozen incident pack from OLAP or TPC-C Now HTML dive (`Save` / `Copy context`).

```json
{
  "schema": "perf-duty-context/v1",
  "report": { "kind": "olap|tpcc", "url": "…", "window": ["…", "…"] },
  "selection": {
    "branch": "main",
    "db": "…",
    "suite": "…",
    "focus_run": {
      "label": "…", "sha": "…", "report": "https://proxy.sandbox…/index.html",
      "ticket_coverage": "uncovered|covered|wrong_branch|ok",
      "uncovered_queries": ["Query01"]
    }
  },
  "suite_now": { },
  "queries": [
    { "test": "Query01", "kind": "fail", "ticket_coverage": "uncovered", "tickets": [] }
  ],
  "sticky_query": null,
  "suite_history": { },
  "compare": {
    "wave_id": "trunk.r…",
    "active": true,
    "run": { "sha": "…", "report": "…", "ticket_coverage": "…", "uncovered_queries": [] },
    "query_counts": { "fail": 0, "nodata": 2 },
    "queries": [],
    "note": "…"
  },
  "ticket_coverage": {
    "status": "uncovered",
    "new_issue_count": 2,
    "uncovered_queries": ["Query01"],
    "wrong_branch_queries": [],
    "covered_queries": ["Query62"],
    "investigate_uncovered_first": true,
    "note": "…"
  },
  "hints": {
    "react": ["fail", "new"],
    "investigate_uncovered_first": true,
    "compare_active": true
  },
  "known_tickets": [
    { "number": 47871, "title": "…", "url": "https://github.com/…/issues/47871", "queries": ["Query12"] }
  ]
}
```

| Field | OLAP | TPC-C |
|-------|------|-------|
| `report.kind` | `olap` | `tpcc` |
| `selection.focus_run.report` | usually sandbox URL | usually sandbox URL (joined from `tests_results`; null only on join miss) |
| `suite_now` | fail_rate / ydb + `n_nodata` / `query_counts` | lat / tpmc |
| `selection.focus_run.success` | SuccessCount for coverage gaps | optional |
| `queries` / `sticky_query` | fail + slow + **nodata samples** + soft; each may carry `ticket_coverage` | empty / null |
| `ticket_coverage` | uncovered / wrong_branch / covered summary — **dig uncovered first** | optional |
| `compare` | when heatmap cmp selected: full `compare.run` + gaps (not only `wave_id`) | optional |
| `known_tickets` | open issues with `perf-duty-match` block, matched to suite@db via `affected` | same |

See also `dutyctl detect-type` seeds: `olap_fail`, `olap_slow`, `olap_nodata`, `tpcc_tpmc`, `tpcc_lat`, `mixed`.  
**Nodata must be packed** (`query_counts.nodata` and/or `queries[].kind=nodata`) — otherwise duty can miss coverage gaps when `issue=ok`.  
**Uncovered first:** if `ticket_coverage.investigate_uncovered_first` or `hints.react` includes `new`, prioritize queries without a branch-matched open issue.  
**Compare (mandatory when set):** if `compare.active` / `hints.compare_active`, dig **`compare.run`** (Allure → `compare_focus.json`, sha, `compare.queries` gaps) **and** `selection.focus_run` (now/latest).  
`detect_type` seeds `seed_compare_*` into `problems_seed` and may add `olap_fail` even when now is only `olap_nodata`.  
`validate` requires «прогон сравнения» + label/sha + log dig for cmp fails. Do **not** stop at now-only.

---

## perf-duty-result/v1 (output)

Written by `dutyctl write-result` (always, even on failure). Agent owns problem conclusions in `problems.json`; tools merge errors/artifacts.

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
  "summary": "one-line plain summary",
  "confidence": "high",
  "confidence_score": 0.85,
  "culprit_found": false,
  "culprit": null,
  "problems": {
    "total": 2,
    "analyzed": 2,
    "unknown": 0,
    "items": []
  },
  "errors": [],
  "warnings": [],
  "artifacts": {
    "analysis_md": "analysis.md",
    "focus_json": "focus.json"
  },
  "timings_sec": {}
}
```

| Field | Meaning |
|-------|---------|
| `ok` | no hard tool/infra failure |
| `status` | `completed` \| `partial` \| `failed` \| `stopped` |
| `resolution` | `update_known` \| `open_ticket` \| `wait_next_wave` \| `investigate_further` \| `no_action` \| `unknown` |
| `culprit_found` | true only if evidence bar met |
| `problems.total` | seeded + discovered |
| `problems.analyzed` | reached a conclusion (incl. unknown) |
| `problems.unknown` | explicitly could not determine |
| `errors[]` | `{ "stage", "message", "retriable" }` |

Human report: `analysis.md` (agent-written). Validate with `dutyctl validate`.  
After creating the issue: `Тикет: #N` → `dutyctl upload-report` → `s3_report.json`  
(`workload-log` …/duty_artifacts/{run_id}/{stamp}/…, boto3) — `[полный отчёт]` in issue Фактура.

Architecture / playbooks: [`AGENTS.md`](AGENTS.md). CLI: `dutyctl.py`.
