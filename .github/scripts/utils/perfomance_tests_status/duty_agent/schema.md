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
    "focus_run": { "label": "…", "sha": "…", "report": "https://proxy.sandbox…/index.html" }
  },
  "suite_now": { },
  "queries": [],
  "sticky_query": null,
  "suite_history": { },
  "links": { "datalens": null },
  "hints": { }
}
```

| Field | OLAP | TPC-C |
|-------|------|-------|
| `report.kind` | `olap` | `tpcc` |
| `selection.focus_run.report` | usually sandbox URL | usually sandbox URL (joined from `tests_results`; null only on join miss) |
| `suite_now` | fail_rate / ydb + `n_nodata` / `query_counts` | lat / tpmc |
| `selection.focus_run.success` | SuccessCount for coverage gaps | optional |
| `queries` / `sticky_query` | fail + slow + **nodata samples** + soft | empty / null |

See also `dutyctl detect-type` seeds: `olap_fail`, `olap_slow`, `olap_nodata`, `tpcc_tpmc`, `tpcc_lat`, `mixed`.  
**Nodata must be packed** (`query_counts.nodata` and/or `queries[].kind=nodata`) — otherwise duty can miss coverage gaps when `issue=ok`.

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

Architecture / playbooks: [`AGENTS.md`](AGENTS.md). CLI: `dutyctl.py`.
