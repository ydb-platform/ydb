# Agent instructions — performance tests status

Toolkit root: `ydb/tools/perfomance_tests_status`

| Request | Subfolder | Instructions |
|---------|-----------|--------------|
| TPC-C / tpcc / lat90 / tpmC | [`tpcc/`](tpcc/) | [`tpcc/AGENTS.md`](tpcc/AGENTS.md) |
| OLAP / Clickbench / Tpch / Tpcds / suites | [`olap/`](olap/) | [`olap/AGENTS.md`](olap/AGENTS.md) |

Route to the matching subfolder and follow its AGENTS.md.

## One-shot local / CI build

```bash
export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
python3 -m pip install 'ydb[yc]>=3.20'
python3 ydb/tools/perfomance_tests_status/run_reports.py --publish-dir /tmp/perf_reports
```

Hourly GitHub Actions: `.github/workflows/perfomance_tests_status.yml`  
Stable S3 URLs (overwritten each run):

- `{AWS_ENDPOINT}/ydb-builds/main/perfomance_tests_status/olap-report.html`
- `{AWS_ENDPOINT}/ydb-builds/main/perfomance_tests_status/tpcc-report.html`

