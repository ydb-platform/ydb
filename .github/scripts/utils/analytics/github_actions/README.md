# github_actions

Колонки GitHub Actions поверх [`collector/`](../collector/README.md): workflow,
job, PR, commit, инвентарь раннера, плюс выгрузка длительностей job и step из
GitHub API после того как run завершился.

Таблица данных — `analytics/ci_metrics`. Служебное состояние выгрузки —
`analytics/ci_metrics_state`, отдельно, чтобы в таблице с данными были только
измерения.

## Что лежит в таблице

Это то, что нужно знать, чтобы написать запрос. `source` говорит, кто написал
строку, `name` — что именно измерено.

| `source` | `name` | Кто пишет | Что это |
| --- | --- | --- | --- |
| `github_job` | `job` | выгрузка | сколько job выполнялся, от `started_at` до `completed_at` |
| `github_job` | `queue` | выгрузка | сколько job ждал раннера; `labels.queued_ms` то же число |
| `github_step` | имя шага | выгрузка | сколько шёл шаг GitHub Actions |
| `ya_phase` | см. ниже | `test_ya` | фазы внутри job |
| `nightly_build` | `ydbd_cached_build`, `ydbd_size` | `nightly_build.yml` | сборка ydbd и размер бинаря |

Имена при `source = ya_phase`:

| `name` | Что измеряет |
| --- | --- |
| `init` | подготовка окружения шага Init |
| `clean_ya_cache`, `setup_cache` | работа с кэшем `ya` |
| `graph_compare` | сравнение графа сборки с базовым коммитом |
| `checkout_head` | переключение на тестируемый коммит |
| `prepare_ya_make` | подготовка перед запуском `ya make` |
| `ya_make_try_N` | вся стена `ya make` попытки N; в `labels.tests` счётчики тестов |
| `ya_build` | локальная компиляция и линковка внутри попытки |
| `ya_tests` | прогон тестов внутри попытки, без интервалов сборки |
| `ya_cache_download`, `ya_cache_upload` | обмен с dist-кэшем, рядом со сборкой |
| `postprocess_try` | постобработка отчёта попытки |
| `transform_build_results`, `fail_checker`, `generate_summary` | обработка отчёта |
| `s3_sync_try` | выгрузка артефактов в S3 |
| `upload_tests_results` | заливка результатов тестов в YDB |
| `runner_info` | разовый снимок cpu/ram/disk раннера, `kind = info` |

`ya_build` / `ya_tests` / `ya_cache_*` считает
[`ya_evlog_phases.py`](ya_evlog_phases.py) по `ya_evlog.jsonl` — те же узлы,
что рисует `ya analyze-make timeline`. Компиляция и линковка это узлы
`Compile`/`Link` и `Run` по объектным файлам; `ya_tests` — все остальные `Run` с
вырезанными интервалами сборки. Попадание в кэш и заливка в кэш сборкой не
считаются, они идут отдельными интервалами, поэтому параллельный fetch виден.

Связь фазы с job: у фаз `labels.parent_span_id = job-{github_job_id}`, а у строки
job `span_id = job-{github_job_id}`. У `queue` `span_id = queue-{job_id}`, у шага
`span_id = step-{job_id}-{N}`.

Схема: PK `(event_ts, date, run_id, github_job_id, run_attempt, source, name, kind, span_id)`,
TTL 1 год на `event_ts`. Без `github_job_id`, `run_attempt` или `span_id` строка в
YDB не уйдёт — сборка при этом не падает, но данные теряются.

## Интеграция в свой workflow

Внутри job — три вызова на span:

```bash
export CI_METRICS_FILE="$TMP_DIR/ci_metrics.jsonl"   # не PUBLIC_DIR: JSONL не должен уехать на публичный S3
CI_METRICS_PY=".github/scripts/utils/analytics/github_actions/ci_metrics.py"

python3 "$CI_METRICS_PY" start my_step --source my_job --label cache_mode=dist_cache
python3 "$CI_METRICS_PY" end my_step --conclusion success
python3 "$CI_METRICS_PY" enrich my_step --label report_url="$S3_URL"   # labels, длительность не меняется
python3 "$CI_METRICS_PY" flush                                        # залить закрытые строки
python3 "$CI_METRICS_PY" send --conclusion cancelled                  # закрыть открытые и залить
```

Нужны: `GITHUB_NUMERIC_JOB_ID` в env (см. ниже) и креды YDB как у collector.

Дополнительно к флагам collector:

- `--runner` — приложить закэшированный инвентарь хоста, в labels появляются
  ключи с префиксом `runner.inventory`.
- `--usage` — свежий замер cpu/ram/disk на это событие, префикс `runner.usage`.
  Кэш инвентаря лежит в `$CI_RUNNER_INFO_FILE`.
- `--report <ya report.json>` у `enrich` — посчитать тесты и положить в
  `labels.tests` (`passed`, `failed`, `errors`, `skipped`, `muted`,
  `not_launched`, `other`, `total`).

### В `test_ya`

Там те же команды завёрнуты в bash-функции, и функции сами добавляют часть
labels:

```bash
record_ci_start <name> [ya_attempt] [source=ya_phase]
record_ci_end   <name> [conclusion=success] [error]
record_ci_end_rc <name> <rc> [what]   # conclusion по реальному коду возврата
record_ci_enrich <name> <args...>
record_ci_flush
```

`record_ci_start` подмешивает `ya_attempt`, `build_target` (из
`$CI_BUILD_TARGET`) и `cache_mode` (из `$CI_CACHE_MODE`), а для основного
build-span ещё `--runner` и `--usage`.

Команда под `|| true` должна закрываться через `record_ci_end_rc`, иначе span
запишется как success при упавшей команде:

```bash
RC=0
some_command || RC=$?
record_ci_end_rc my_step "$RC" my_step
```

Спаны, оставшиеся открытыми из-за `set -e` или отмены job, закрывают
`trap ... EXIT` и `trap ... TERM INT`.

Частые labels: `cache_mode`, `ya_attempt`, `build_target`, `report_url`,
`ya_make_log_url`, `artifacts_url`, `tests`, `tests_status`, `failed_tests`,
`error`, `parent_span_id`.

## Env → колонки

Заполняет шаг `Resolve analytics job name` в `test_ya` либо сам workflow.

| Колонка | Откуда |
| --- | --- |
| `run_id` | `GITHUB_RUN_ID` |
| `github_job_id` | `GITHUB_NUMERIC_JOB_ID` |
| `run_attempt` | `GITHUB_RUN_ATTEMPT` |
| `job_name` | `CI_JOB_TITLE` или `GITHUB_JOB` |
| `workflow` | `GITHUB_WORKFLOW` |
| `event_name` | `GITHUB_EVENT_NAME` |
| `branch` | `BRANCH_NAME` / `GITHUB_BASE_REF` / event / `GITHUB_REF_NAME` |
| `commit` | `ORIGINAL_HEAD` / event / `GITHUB_SHA` |
| `pr_number` | `PR_NUMBER` или event |
| `build_preset` | `BUILD_PRESET`; в выгрузке — regex по имени job |
| `run_url` | `GITHUB_REPOSITORY` + `GITHUB_RUN_ID` |

`GITHUB_NUMERIC_JOB_ID` в `test_ya` получает
[`resolve_github_job_id.py`](../../analytics/resolve_github_job_id.py): берёт
строку jobs API с `runner_name` равным `$RUNNER_NAME`. Завершённые job не
рассматриваются — имена раннеров переиспользуются, и завершённый job это
предыдущий арендатор. Если API ещё не догнал, скрипт повторяет попытку с
backoff; когда не получилось совсем, печатает annotation, потому что без этого
id теряются все строки job.

Файл буфера — `CI_METRICS_FILE`, остальное про буфер и креды в
[collector](../collector/README.md#файлы-на-диске).

## Таблицы

Создаются один раз, не на записи:

```bash
python3 .github/scripts/utils/analytics/github_actions/provision_tables.py
```

`analytics/ci_metrics_state` — три строки с JSON:

| `name` | Что внутри |
| --- | --- |
| `export_watermark` | докуда выгрузка дошла, `exported_until` |
| `open_runs` | run, которые ещё шли, их надо перечитать когда завершатся |
| `failed_runs` | run, у которых не удалось получить список job, со счётчиком попыток |

## Выгрузка job и step

Отдельный job в `collect_analytics_fast.yml` (не на раннере сборки):

```bash
export GITHUB_TOKEN=...
python3 .github/scripts/utils/analytics/github_actions/export_github_job_metrics.py --hours 2
```

- По умолчанию все активные workflow; `--workflow` или `CI_METRICS_WORKFLOW`
  сужает список (можно через запятую), `--org` / `CI_METRICS_ORG`,
  `--repo` / `CI_METRICS_REPO`, `--table-path` — по желанию.
- Окно `created` для завершённых run: на холодном старте `--hours`, дальше от
  `export_watermark` минус 30 минут. Ограничения по глубине нет: если выгрузка
  стояла сутки, она продолжит с того места, где встала.
- Run, которые ещё идут, запоминаются в `open_runs` и дочитываются по id при
  завершении — по сохранённой попытке, а не по текущей.
- Уже записанные `github_job_id` пропускаются, поэтому re-run failed jobs не
  теряется.
- Watermark двигается только когда окно прочитано целиком. Упёрлись в
  rate limit или не смогли получить список run — watermark остаётся, следующий
  запуск перечитает то же окно. Выгрузка при этом возвращает ненулевой код,
  чтобы падение было видно.

## Тесты

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/ci -p 'test_*.py'
```
