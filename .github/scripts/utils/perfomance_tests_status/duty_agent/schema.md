# perf-duty-context/v1

Frozen incident pack from OLAP or TPC-C Now HTML dive.

```json
{
  "schema": "perf-duty-context/v1",
  "generated_at": "2026-07-24T12:00:00.000Z",
  "report": {
    "kind": "olap",
    "url": "https://…/olap-report.html",
    "window": ["2026-06-24", "2026-07-24"],
    "generated_at": "…",
    "source": "perfomance/olap/…"
  },
  "selection": {
    "branch": "main",
    "db": "sas_small_column",
    "suite": "TpchParallelS100T10",
    "family": "Tpch",
    "focus_run": {
      "label": "2026-07-08_da957bd",
      "ci_version": "trunk.r20245012",
      "sha": "da957bd",
      "ts": "2026-07-08T05:32:58",
      "report": "https://proxy.sandbox.yandex-team.ru/…/index.html",
      "fail_rate": 1.0
    }
  },
  "suite_now": {
    "issue": "failing",
    "status": "failing",
    "fail_rate_now": 0.38,
    "fail_rate_base": 0.38,
    "reasons": []
  },
  "queries": [
    { "test": "Query05", "kind": "fail", "error_class": "other" }
  ],
  "sticky_query": "Query05",
  "sticky_detail": null,
  "suite_history": { "labels": [], "fail_rate": [], "versions": [] },
  "compare": { "wave_id": null },
  "links": { "datalens": null },
  "hints": { "react": ["fail"], "wave_view": "finished" }
}
```

## Kind differences

| Field | OLAP | TPC-C |
|-------|------|-------|
| `report.kind` | `olap` | `tpcc` |
| `selection.focus_run.report` | usually sandbox URL | often null |
| `selection.focus_run.ci_version` | yes | — (use `day`) |
| `suite_now` metrics | fail_rate / ydb | lat / tpmc |
| `queries` / `sticky_query` | yes | empty / null |
| `links.datalens` | — | optional |

Harness must accept both kinds without requiring sandbox URL.
