# Agent instructions — performance tests status

Toolkit root: `.github/scripts/utils/perfomance_tests_status`

| Request | Subfolder | Instructions |
|---------|-----------|--------------|
| TPC-C / tpcc / lat90 / tpmC | [`tpcc/`](tpcc/) | [`tpcc/AGENTS.md`](tpcc/AGENTS.md) |
| OLAP / Clickbench / Tpch / Tpcds / suites | [`olap/`](olap/) | [`olap/AGENTS.md`](olap/AGENTS.md) |
| Duty context pack / investigate incident | [`duty_agent/`](duty_agent/) | [`duty_agent/AGENTS.md`](duty_agent/AGENTS.md) |

Route to the matching subfolder and follow its AGENTS.md.

Duty: dive **Save context** / **Copy context** → JSON →  
`cd duty_agent && eval "$(python3 dutyctl.py init-token --shell)"`  
(YAV → `SANDBOX_TOKEN` + YDB SA + `AWS_KEY_*` for `workload-log`) → follow [`duty_agent/AGENTS.md`](duty_agent/AGENTS.md).  
After creating the issue: put `Тикет: #N` in analysis → `dutyctl upload-report -o $OUT`  
(immutable S3 stamp under `workload-log` + `[полный отчёт]` in issue body; needs `boto3`).  
Mart fetch: [`common/ydb_client.py`](common/ydb_client.py) (not MCP).  
Report defaults (window, baseline, focus dbs, …): `tpcc/report_config.json`, `olap/report_config.json`.

## One-shot local / CI build

```bash
# Local: eval "$(python3 duty_agent/dutyctl.py init-token --shell)"
# or: export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
python3 -m pip install 'ydb[yc]>=3.20'
python3 .github/scripts/utils/perfomance_tests_status/run_reports.py --publish-dir /tmp/perf_reports
```

Hourly GitHub Actions: `.github/workflows/perfomance_tests_status.yml`  
Stable S3 URLs (overwritten each successful run; OLAP/TPC-C publish independently):

- `{AWS_ENDPOINT}/ydb-builds/main/perfomance_tests_status/olap-report.html`
- `{AWS_ENDPOINT}/ydb-builds/main/perfomance_tests_status/tpcc-report.html`

Duty notes: stale uses wall-clock; Now = last completed vs median(prev7); failing queries show mart error class (`timeout`/`diff`/`other` via Color).

## Local unit tests (classify / compare)

```bash
# duty issue match blocks
cd .github/scripts/utils/perfomance_tests_status
python3 common/tests/test_duty_issues.py

# OLAP Now / compare
cd .github/scripts/utils/perfomance_tests_status/olap
python3 tests/test_classify_rules.py

# TPC-C compare-delta (mirror of template.html)
cd .github/scripts/utils/perfomance_tests_status/tpcc
python3 tests/test_classify_rules.py

# Duty agent
cd .github/scripts/utils/perfomance_tests_status/duty_agent
python3 tests/test_duty_agent.py
```

