# Agent instructions — performance tests status

Toolkit root: `.github/scripts/utils/perfomance_tests_status`

| Request | Subfolder | Instructions |
|---------|-----------|--------------|
| TPC-C / tpcc / lat90 / tpmC | [`tpcc/`](tpcc/) | [`tpcc/AGENTS.md`](tpcc/AGENTS.md) |
| OLAP / Clickbench / Tpch / Tpcds / suites | [`olap/`](olap/) | [`olap/AGENTS.md`](olap/AGENTS.md) |
| Duty context pack / investigate incident | [`duty_agent/`](duty_agent/) | [`duty_agent/AGENTS.md`](duty_agent/AGENTS.md) |

Route to the matching subfolder and follow its AGENTS.md.

Duty: dive **Save context** / **Copy context** → `perf-duty-context/v1` JSON →  
`python3 duty_agent/run.py --context pack.json --out duty-card.md` (works for olap + tpcc).

## One-shot local / CI build

```bash
export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
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
cd .github/scripts/utils/perfomance_tests_status/olap
python3 test_classify_rules.py
```

