# Analytics client

Два слоя:

| Файл | Роль |
| --- | --- |
| `core.py` | общее ядро: `start` / `end` / `track` / `send`, JSONL, flush в YDB. Без GitHub. Можно копировать в Arcadia / LLM-пайплайн. |
| `ci_metrics.py` | обёртка: GitHub / job / PR / `github.event.*`, колонки CI-таблицы, `--runner` / `--usage` |
| `runner_info.py` | инвентарь хоста (кэш) и usage-снимок |
| `export_ya_nodes.py` | сырые узлы evlog / clang time-trace + `build_info` |
| `export_github_job_metrics.py` | queue / job / GHA-step после факта |

`YDBWrapper` по-прежнему из `.github/scripts/analytics/ydb_wrapper.py`.

## Core (Arcadia / LLM / любой CI)

Нет GitHub-контекста. `run_id` — только из `--run-id` или `$ANALYTICS_RUN_ID` (не `$GITHUB_RUN_ID`). Файл — `$ANALYTICS_FILE`. Свой контекст можно передать `--json` / `--attr` или `attach=` в Python.

```bash
python3 .github/scripts/utils/analytics/core.py start llm_call \
  --source arcadia --run-id "$TASK_ID" --attr model=foo
# ... work ...
python3 .github/scripts/utils/analytics/core.py send --conclusion success \
  --json '{"tokens":12,"latency_ms":340}'
```

```python
from core import Analytics

analytics = Analytics(file="llm.jsonl", source="arcadia")
analytics.start("llm_call", {"model": "foo"})
analytics.send(conclusion="success", properties={"tokens": 12})
```

Таблица ядра: `analytics/events`, PK `(event_ts, date, run_id, source, name, kind)` — `event_ts` первым, иначе column-store не даёт TTL. Жирные дампы — `kind=info`, имя по умолчанию `info`, тело в `labels.payload`.

## CI wrapper (этот репозиторий)

Базовый GitHub-контекст подставляется сам. One-liner в workflow:

```bash
python3 .github/scripts/utils/analytics/ci_metrics.py start my_step \
  --source my_wf --attr cache_mode=dist_cache --runner
# ... work ...
python3 .github/scripts/utils/analytics/ci_metrics.py end my_step --conclusion success --error "rc=1" --usage
# later, after S3 upload — duration stays, labels gain the URL
python3 .github/scripts/utils/analytics/ci_metrics.py enrich my_step --label report_url="$S3_URL"
python3 .github/scripts/utils/analytics/ci_metrics.py send
```

`--runner` / `--usage` — только в обёртке (инвентарь хоста и свежий usage). Nightly `ydbd_cached_build` и clean `ydbd_clean_build` передают `--runner` на `start` и `--usage` на `end`.

Таблица CI: `analytics/ci_metrics` (колонки workflow / job / PR / commit + `github.*` в labels).

Что пишется из GitHub Actions:

- `export_github_job_metrics.py`: queue / job / GHA-step по **всем активным** workflow (PR-check, Run-tests, nightly, …). Один файл: `--workflow pr_check.yml` или `CI_METRICS_WORKFLOW=pr_check.yml`.
- PR-check in-job: ya phases с раннера
- Nightly-Build: `ydbd_cached_build`, `ydbd_size`, evlog + `build_info`
- ydbd-clean-build: то же без кеша
- Build-analytics-run: clang time-trace + evlog
