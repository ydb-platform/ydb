# github_actions

Обёртка над `collector/` для GitHub Actions: колонки workflow / job / PR / commit, `--runner` / `--usage`, выгрузка job/step после факта.

Таблица: `analytics/ci_metrics`. Профили компиляции сюда не кладём.

PK: `(event_ts, date, run_id, github_job_id, run_attempt, source, name, kind, span_id)`. Без `github_job_id` / `run_attempt` / `span_id` строка в YDB не уходит. TTL — 1 год. `CREATE TABLE` на flush не вызывается (`ensure_table=False`).

Связка с GitHub job: в labels пишется `parent_span_id=job-{github_job_id}` (то же у export `queue`/`step`). Job id — строка jobs API, у которой `runner_name` равен `$RUNNER_NAME` (скрипт `resolve_github_job_id.py`, страницы по 100).

## CLI в job

```bash
export CI_METRICS_FILE="$TMP_DIR/ci_metrics.jsonl"   # не PUBLIC_DIR: jsonl не должен уезжать на публичный S3
CI_METRICS_PY=".github/scripts/utils/analytics/github_actions/ci_metrics.py"

python3 "$CI_METRICS_PY" start my_step --source ya_phase --label cache_mode=dist_cache
python3 "$CI_METRICS_PY" end my_step --conclusion success
python3 "$CI_METRICS_PY" enrich my_step --label report_url="$S3_URL"
python3 "$CI_METRICS_PY" flush          # только закрытые строки
python3 "$CI_METRICS_PY" send           # закрыть хвосты и выгрузить
python3 "$CI_METRICS_PY" enrich ya_make_try_1 --label ya_attempt=1 --report "$CURRENT_REPORT"
```

`--runner` — один раз снять cpu/ram/disk хоста и повесить на событие. `--usage` — свежий snapshot на это событие.

В `test_ya` то же самое через `record_ci_start` / `record_ci_end` / `record_ci_enrich` / `record_ci_flush`. Новый спан — три строки: start, end, при необходимости enrich и flush. На cancel: `trap TERM INT` → `send --conclusion cancelled`.

Типичные labels: `cache_mode`, `ya_attempt`, `report_url`, `error`, `tests_status`, `failed_tests`.

## Env → колонки

Ставит `test_ya` (шаг Resolve analytics job name) или сам workflow.

| Колонка | Откуда |
| --- | --- |
| `run_id` | `GITHUB_RUN_ID` |
| `github_job_id` | `GITHUB_NUMERIC_JOB_ID` (без него строки не пишутся в YDB; сборка не падает) |
| `job_name` | `CI_JOB_TITLE` или `GITHUB_JOB` |
| `workflow` | `GITHUB_WORKFLOW` |
| `event_name` | `GITHUB_EVENT_NAME` |
| `branch` | `BRANCH_NAME` / `GITHUB_BASE_REF` / event / `GITHUB_REF_NAME` |
| `commit` | `ORIGINAL_HEAD` / event / `GITHUB_SHA` |
| `pr_number` | `PR_NUMBER` или event |
| `build_preset` | `BUILD_PRESET` (в job; в export — regex по имени job) |
| `run_attempt` | `GITHUB_RUN_ATTEMPT` |

Файл буфера: `CI_METRICS_FILE`. Креды склада — как у collector.

## Job/step после факта

Отдельный job (`collect_analytics_fast.yml` → `github_job_metrics`), не из раннера:

```bash
export GITHUB_TOKEN=...
python3 .github/scripts/utils/analytics/github_actions/export_github_job_metrics.py \
  --hours 2
```

Окно GitHub `created` всегда `--hours` (в cron — 36), его watermark не сужает. Уже записанные `github_job` с `event_ts` в этом окне пропускаются. Ошибка запроса — выгружаем окно целиком, upsert идемпотентный. По умолчанию все active workflows. `--workflow pr_check.yml` ограничивает список. `--org` / `--repo` / `--table-path` по желанию.

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/ci -p 'test_*.py'
```
