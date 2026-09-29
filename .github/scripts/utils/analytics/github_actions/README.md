# github_actions

Обёртка над [`collector/`](../collector/README.md) для GitHub Actions. Пишет
в ту же схему плюс колонки workflow / job / PR / commit и умеет выгрузить из
GitHub API, сколько шли job и step.

Две таблицы:

- `analytics/ci_metrics` — то, что рисует дашборд: длительности фаз, job, step.
- `analytics/ci_metrics_state` — записная книжка крона: «я уже выгрузил всё
  до 15:00, эти run ещё идут, те надо повторить». Дашборд её не читает.
  Отдельная таблица, чтобы это не лежало рядом с измерениями и не попало
  на график.

## Добавить измерение

Чтобы на дашборде связать вашу фазу с job, в строке должны быть два числа.

**Номер job в GitHub** (колонка `github_job_id`, например `456`). GitHub сам
его в env не кладёт — только строковое имя в `$GITHUB_JOB` (`build`). Число
нужно, чтобы потом сджойнить фазу с строкой job, которую выгрузка пишет как
`span_id = job-456`. В `test_ya` его находит
[`resolve_github_job_id.py`](../../analytics/resolve_github_job_id.py) и кладёт
в `$GITHUB_NUMERIC_JOB_ID`. В своём workflow выставьте то же сами или
вызовите этот скрипт.

**Номер попытки workflow** (колонка `run_attempt`). GitHub кладёт его сам в
`$GITHUB_RUN_ATTEMPT`: `1` с первого раза, `2` после Re-run. Нужен, чтобы
повторный прогон не затёр первый: оба числа входят в первичный ключ.

Нет любого из двух — строка не попадёт в таблицу, уйдёт в файл
`$CI_METRICS_FILE.skipped` рядом с JSONL. Сборка не упадёт.

JSONL не кладите в каталог, который `test_ya` выкладывает на публичный S3
(`PUBLIC_DIR`) — иначе файл с метриками уедет в интернет.

### Своё измерение в workflow

```bash
export CI_METRICS_FILE="$TMP_DIR/ci_metrics.jsonl"
PY=.github/scripts/utils/analytics/github_actions/ci_metrics.py

python3 "$PY" start compile --source my_workflow --label cache_mode=dist_cache
# … работа …
compile
RC=$?
python3 "$PY" end compile --rc "$RC"         # exit code той команды: 0 → success
python3 "$PY" enrich compile --label report_url="$URL"
python3 "$PY" flush
```

`--source` — кто пишет (ваш workflow или `ya_phase`). `--name` (`compile`) — что
измерено. Новые значения сначала внесите в [`taxonomy.py`](taxonomy.py) и в
таблицы ниже: тест сверит README с кодом.

Дополнительно к collector:

- `--runner` на `start` — какая машина: сколько ядер, сколько RAM, сколько
  диска. Спецификация железа, один раз за job.
- `--usage` на `end` — сколько из этого занято **после** работы: загрузка
  CPU, сколько RAM и диска осталось свободным. Чтобы на дашборде видеть
  «бокс на 64 ГБ, а мы сожрали 60».
- `--report <ya report.json>` на `enrich` — из отчёта `ya` в labels
  (`passed`, `failed`, `errors`, `skipped`, `muted`, `not_launched`,
  `other`, `total`)
- `--rc N` — exit code той команды, которую вы только что измерили
  (в bash это `$?`). `0` → в колонку `conclusion` пишем `success`, любое
  другое число → `failure` и в labels `error` вроде `compile rc=1`. Без
  `--rc` пришлось бы руками писать `--conclusion success` и забывать, когда
  команда упала.
- `--ya-attempt N` или `$CI_YA_ATTEMPT` — какая это попытка `ya make`
  (1, 2, 3…), если job ретраит сборку. Не путать с `$GITHUB_RUN_ATTEMPT`
  (это повтор всего workflow).

### Что окажется в `analytics/ci_metrics`

Вы передаёте `name`, `source`, labels, rc. Остальное collector и обёртка
дописывают сами.

```sql
SELECT name, source, kind, value, unit, conclusion,
       workflow, job_name, github_job_id, run_attempt, pr_number, labels
FROM `analytics/ci_metrics`
WHERE run_id = 123 AND name = "compile";
```

| Колонка | Откуда | Пример |
| --- | --- | --- |
| `name` | аргумент | `compile` |
| `source` | `--source` | `my_workflow` |
| `kind` / `value` / `unit` | collector | `duration` / `18400` / `ms` |
| `conclusion` | `--rc` / `--conclusion` | `success` |
| `run_id` | `$GITHUB_RUN_ID` | `123` |
| `github_job_id` | `$GITHUB_NUMERIC_JOB_ID` | `456` |
| `run_attempt` | `$GITHUB_RUN_ATTEMPT` | `1` |
| `workflow` | `$GITHUB_WORKFLOW` | `PR-check` |
| `job_name` | `$CI_JOB_TITLE` или `$GITHUB_JOB` | `build-relwithdebinfo` |
| `event_name` | `$GITHUB_EVENT_NAME` | `pull_request` |
| `branch` | `$BRANCH_NAME` / `$GITHUB_BASE_REF` / event | `main` |
| `commit` | `$ORIGINAL_HEAD` / `$GITHUB_SHA` | `abc…` |
| `pr_number` | `$PR_NUMBER` или event | `54142` |
| `build_preset` | `$BUILD_PRESET` | `relwithdebinfo` |
| `run_url` | репозиторий + `run_id` | `https://github.com/…/actions/runs/123` |
| `span_id` | collector | id этой строки (случайный hex) |
| `labels.parent_span_id` | обёртка | `job-456` (у самой строки job не ставится) |
| `labels.cache_mode` | `--label` / `$CI_CACHE_MODE` | `dist_cache` |

Первичный ключ:
`(event_ts, date, run_id, github_job_id, run_attempt, source, name, kind, span_id)`.
Строки живут год. У выгрузки из GitHub API `span_id` не случайный, а
`job-{id}`, `queue-{id}`, `step-{id}-{N}` — по нему фазу джойнят с job.

### Новая фаза в `test_ya`

1. Имя в `YA_PHASE_NAMES` в [`taxonomy.py`](taxonomy.py).
2. Строка в таблице фаз ниже (тот же `name`).
3. В action:

```bash
ci start my_new_phase
# …
ci end my_new_phase --rc "$RC"
```

`ci` — короткая функция в `action.yml`: вызывает `ci_metrics.py` и глотает
его ошибку (`|| true`), чтобы сломанная аналитика не валила сборку.

В labels сами допишутся, если переменные заданы: номер попытки `ya make`
(`$CI_YA_ATTEMPT`), цель сборки (`$CI_BUILD_TARGET`), режим кэша
(`$CI_CACHE_MODE`). Их не нужно передавать в каждую команду.

`$CI_BUILD_SPAN` — как назвать главную стену `ya make`. Если не задана,
фаза называется `ya_make_try_1`, `ya_make_try_2`, … Если задать, например,
`ydbd_cached_build`, вокруг `ya make` будут `start`/`end` с этим именем.
Только для этой фазы Python сам снимет машину: на старте сколько ядер и
RAM (`--runner`), в конце сколько занято (`--usage`). На `init` и
`checkout` это не нужно — зря тратить время.

В шаге стоит `set -e`: любая упавшая команда сразу выходит из скрипта.
Тогда `ci start` уже вызван, а до `ci end` дело не дойдёт — в таблице
повиснет дыра без длительности и без `failure`. Python этого не видит:
он не исполняет bash. Поэтому в начале шага два хука (`trap` — «когда
случится X, выполни вот это»):

- GitHub прислал TERM/INT (job отменили) — закрыть всё как `cancelled` и
  залить.
- Шаг завершается по любой причине, в том числе из‑за `set -e` — закрыть
  незакрытые `start` с тем exit code, с которым вышел скрипт, и залить.

Это уже стоит в `test_ya`. В свой workflow скопируйте тот же каркас, если
там тоже `set -e`.

```bash
CI_METRICS_PY=".github/scripts/utils/analytics/github_actions/ci_metrics.py"
ci() { python3 "$CI_METRICS_PY" "$@" || true; }
trap 'trap - EXIT; ci send --conclusion cancelled' TERM INT
trap 'rc=$?; trap - EXIT; ci send --rc "$rc"; exit $rc' EXIT
```

## Что уже пишется

`source` — кто написал строку, `name` — что измерено.

| `source` | `name` | Кто пишет | Что это |
| --- | --- | --- | --- |
| `github_job` | `job` | выгрузка | сколько job выполнялся, от `started_at` до `completed_at` |
| `github_job` | `queue` | выгрузка | сколько job ждал раннера; `labels.queued_ms` то же число |
| `github_step` | имя шага | выгрузка | сколько шёл шаг GitHub Actions |
| `ya_phase` | см. ниже | `test_ya` | фазы внутри job |
| `nightly_build` | `ydbd_cached_build`, `ydbd_size` | `nightly_build.yml` | сборка ydbd и размер бинаря |

| `name` | Что измеряет |
| --- | --- |
| `init` | шаг Init: каталоги, креды, S3, `GITHUB_NUMERIC_JOB_ID` |
| `clean_ya_cache` | чистка локального кэша `ya` |
| `setup_cache` | подключение dist-кэша / bazel-remote |
| `graph_compare` | сравнение графа сборки с базовым коммитом |
| `checkout_head` | `git checkout` на коммит, который тестируем |
| `prepare_ya_make` | флаги, mute-лист, каталоги — всё сразу перед `ya make` |
| `ya_make_try_N` | весь `ya make` попытки N (сборка + тесты). Счётчики тестов — в `labels.tests` |
| `ya_build` | из evlog: локальная компиляция и линковка внутри этой попытки |
| `ya_tests` | из evlog: прогон тестов внутри попытки, без интервалов сборки |
| `ya_cache_download` | из evlog: скачивание из dist-кэша во время сборки |
| `ya_cache_upload` | из evlog: заливка в dist-кэш во время сборки |
| `postprocess_try` | сразу после `ya make`, ещё до «сборка красная / зелёная»: разобрать evlog на `ya_build`/`ya_tests`, вытащить OOM из dmesg, timeline, отчёт по RAM, test_bloat |
| `transform_build_results` | дописать в `report.json` ссылки на логи, мьюты, артефакты — чтобы HTML-отчёт был читаемый |
| `fail_checker` | посчитать упавшие тесты и решить, ретраить ли попытку |
| `generate_summary` | собрать текст комментария в PR («Tests passed/failed» и ссылки) |
| `s3_sync_try` | залить `PUBLIC_DIR` на публичный S3, чтобы ссылки из комментария открывались |
| `upload_tests_results` | залить результаты тестов в таблицу `test_results` (это не `ci_metrics`) |
| `runner_info` | одна строка на весь job, **до** цикла `ya make`: сколько ядер, RAM и диска у машины. Не длительность (`kind = info`), потому что железо за job не меняется. На дашборде: отфильтровать «медленно на 8 ядрах» от «медленно на 64». Не путать с `--runner`/`--usage` на фазе сборки — те пишутся в labels той фазы (спека на старте, занятость в конце) |

`ya_build` / `ya_tests` / `ya_cache_*` считает
[`ya_evlog_phases.py`](ya_evlog_phases.py) по `ya_evlog.jsonl` — те же узлы,
что рисует `ya analyze-make timeline`.

## Выгрузка job и step

Отдельный job в `collect_analytics_fast.yml`, не на раннере сборки. Таблицы
создаёт `provision_tables.py` до выгрузки.

```bash
export GITHUB_TOKEN=...
python3 .github/scripts/utils/analytics/github_actions/provision_tables.py
python3 .github/scripts/utils/analytics/github_actions/export_github_job_metrics.py --hours 2
```

- По умолчанию все активные workflow; `--workflow` / `CI_METRICS_WORKFLOW`
  сужает список.
- Первый запуск смотрит `--hours` часов назад. Дальше продолжает с того
  места, где остановился в прошлый раз (поле `export_watermark` в
  `analytics/ci_metrics_state`), с запасом 30 минут.
- Ещё не закончившиеся run запоминаются в `open_runs` и читаются снова в
  следующий раз, той же попыткой.
- Уже записанный `github_job_id` пропускается (re-run failed jobs не теряется).
- Метка «досюда выгрузили» сдвигается только если всё окно прочитано без
  ошибок. Rate limit или сбой списка — ненулевой код, следующее окно то же.

В `analytics/ci_metrics_state` три поля: `export_watermark` (до какого
момента выгрузили), `open_runs` (ещё идут), `failed_runs` (надо повторить).

## Миграция живой таблицы

Разовый ремонт уже записанных строк: `migrate_ci_metrics.py`. Без `--apply`
ничего не пишет. Команды — в docstring скрипта. Живую таблицу этот PR не
переписывает.

## Тесты

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/ci -p 'test_*.py'
```
