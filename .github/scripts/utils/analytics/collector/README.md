# collector

Нужно понять, **сколько времени занял кусок работы**, и потом это выбрать
в SQL / нарисовать на дашборде. Не логи, не трейсы: одна закрытая операция —
одна строка в YDB (`compile` шёл 15 секунд, `success`).

Зачем отдельная папка, а не `INSERT` прямо из скрипта:

- Скрипт может упасть до записи в базу. Сначала пишем на диск (JSONL), в
  базу заливаем пачкой в конце (`flush`). Незакрытый `start` в таблицу не
  едет — не будет строки без длительности.
- Аналитика не должна валить сборку: нет кредов или SDK — warning и выход 0.
  Кривая строка уходит в `.skipped`, процесс зелёный.
- Один и тот же запуск (`run_id`) можно потом выбрать целиком:
  `WHERE run_id = 42`.
- Про GitHub, `ya` и CI collector не знает. В YDB CI поверх него лежит
  [`github_actions/`](../github_actions/README.md). В другой проект
  копируется эта папка.

Пользоваться стоит, если вы уже меряете время `echo`/`date` или пишете своё
логирование и хотите те же числа в одной таблице, а не в логах раннера.

## Как подключить

1. Скопируйте `collector/` и положите на `PYTHONPATH` каталог, в котором она лежит.
2. Рядом нужен модуль `ydb_wrapper` (см. ниже) и пакет `ydb`.
3. Задайте `ANALYTICS_FILE` — локальный JSONL. И `ANALYTICS_RUN_ID` — номер
   этого запуска, чтобы потом выбрать все его события (`WHERE run_id = …`).
   Любое целое: id пайплайна, timestamp, счётчик. В GitHub Actions обёртка
   подставляет `GITHUB_RUN_ID` сама.
4. Вызовите `start` → работа → `end` → `flush`.

```bash
export PYTHONPATH=/opt/analytics          # здесь лежит пакет collector/
export ANALYTICS_FILE=/tmp/analytics.jsonl
export ANALYTICS_RUN_ID=42               # все события этого запуска получат run_id=42
export ANALYTICS_YDB_CREDENTIALS=/path/to/sa.json

python3 -m collector start compile --source my_pipeline --label cache=hit
# … работа …
python3 -m collector end compile --conclusion success
python3 -m collector flush
```

`--file` перекрывает `ANALYTICS_FILE`.

То же из Python:

```python
from collector import start, end, flush_file

start("compile", source="my_pipeline", labels={"cache": "hit"})
end("compile", conclusion="success")
flush_file()
```

## Что окажется в таблице

Пишет в `analytics/events`. `start compile` запоминает время, `end compile`
считает длительность, `flush` пишет **одну строку**. Пока нет `end`, в таблицу
ничего не едет. `track` — сразу готовое число, без пары start/end.

| Колонка | В примере | Кто заполняет |
| --- | --- | --- |
| `date` | `2026-09-21` | из `event_ts` |
| `event_ts` | момент `start` | collector |
| `run_id` | `42` | id запуска: `ANALYTICS_RUN_ID` / `--run-id` |
| `name` | `compile` | первый аргумент |
| `kind` | `duration` | `--kind`, по умолчанию duration |
| `source` | `my_pipeline` | `--source`, без него строка не пишется |
| `span_id` | случайный hex | id этой строки, чтобы два `compile` в одном запуске не слились |
| `value` | `15000` | длительность в мс; для gauge/count — `--value` |
| `unit` | `ms` | из `kind`, либо `--unit` |
| `conclusion` | `success` | `--conclusion` на `end` / `send` |
| `labels` | `{"cache":"hit"}` | `--label` / `enrich` |
| `exported_at` | время `flush` | collector |

Других колонок нет. Job, PR, workflow — это уже обёртка
[`github_actions/`](../github_actions/README.md).

```sql
SELECT name, source, value, unit, conclusion, labels
FROM `analytics/events`
WHERE run_id = 42 AND name = "compile";
```

Строка без `name`, `source`, `run_id`, `event_ts` или `span_id` в таблицу не
пойдёт: она окажется в `$ANALYTICS_FILE.skipped` с полем `reason`. Сборка при
этом не падает.

## Команды

| Команда | Что делает |
| --- | --- |
| `start NAME --source S` | запомнить время старта |
| `end NAME --conclusion …` | посчитать `value` и закрыть |
| `track NAME --source S --value N` | записать готовое число, без `start` |
| `enrich NAME --label k=v` | дописать labels в последнюю незалитую строку с этим именем. Длительность, `event_ts`, `conclusion` не трогает |
| `flush` | залить только закрытые строки |
| `send` | закрыть незакрытые `start` и залить всё |

`flush` и `send` можно вызывать сколько угодно раз: уже залитое повторно не едет.

Флаги: `--kind duration\|gauge\|count\|event\|info`, `--value`, `--unit`,
`--duration-ms`, `--started-epoch`, `--finished-epoch`, `--conclusion`,
`--error`, `--label key=value` (повторяемый), `--run-id`, `--file`.

## Файлы рядом с буфером

Рядом с `$ANALYTICS_FILE` collector держит служебные файлы:

| Файл | Зачем |
| --- | --- |
| `$ANALYTICS_FILE` | буфер, по строке на событие |
| `.offset` | сколько байт уже залито |
| `.pending` | незакрытые `start`; поэтому `flush` их не заливает |
| `.skipped` | отвергнутые строки + `reason` |
| `.lock` | чтобы `enrich` и запись в файл не пересеклись |

Если файл оборвался посередине строки, `flush` эту строку не заливает.
Следующая запись допишет перевод строки, чтобы новая не приклеилась к обрывку.

## Заливка в YDB

Пишет в таблицу `analytics/events`. Строки живут год, потом удаляются.
Первый `flush` создаёт таблицу сам, если её ещё нет.

Ключ сервисного аккаунта — `ANALYTICS_YDB_CREDENTIALS` (путь к json). В этом
репозитории тот же файл уже лежит в
`CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` — его выставляет setup action.

Нет ключа или нет пакета `ydb` — `flush` пишет warning и выходит 0. Сборка
зелёная, в базу ничего не попало.

В этом репозитории путь таблицы задаётся в
[`.github/config/ydb_qa_config.json`](../../../../config/ydb_qa_config.json):
`"analytics_events": "analytics/events"`. Это тот же словарь, из которого
другие QA-скрипты берут `ci_metrics` → `analytics/ci_metrics`. В другом
проекте конфига нет — collector просто пишет в `analytics/events`.

Рядом должен быть модуль `ydb_wrapper` с классом `YDBWrapper`. В этом
репозитории файл уже есть: `.github/scripts/analytics/ydb_wrapper.py`.
Collector сначала делает обычный `import ydb_wrapper`, если не нашёл —
подхватывает этот файл. Класс должен уметь:

```python
class YDBWrapper:
    def __enter__(self) -> "YDBWrapper": ...
    def __exit__(self, exc_type, exc, tb) -> bool: ...
    def check_credentials(self) -> bool: ...
    def get_table_path(self, table_name: str) -> str: ...
    def create_table(self, table_path: str, create_sql: str) -> None: ...
    def bulk_upsert_batches(self, table_path, rows, column_types, batch_size=1000) -> None: ...
```

`get_table_path("analytics_events")` должен вернуть путь таблицы. Нет такого
имени в конфиге — collector возьмёт `analytics/events`.

## Тесты

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/collector -p 'test_*.py'
```
