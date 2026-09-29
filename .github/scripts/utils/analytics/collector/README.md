# collector

JSONL-буфер и заливка в YDB. Про GitHub, `ya` и CI ничего не знает: в другой
проект копируется эта папка.

## Как подключить

1. Скопируйте `collector/` и положите на `PYTHONPATH` каталог, в котором она лежит.
2. Рядом должен импортироваться `ydb_wrapper` (см. ниже) и стоять SDK `ydb`.
3. Задайте `ANALYTICS_FILE` (локальный JSONL) и `ANALYTICS_RUN_ID` — номер
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

`--file` перекрывает `ANALYTICS_FILE`. Старое имя `CI_METRICS_FILE` ещё читается.

То же из Python:

```python
from collector import start, end, flush_file

start("compile", source="my_pipeline", labels={"cache": "hit"})
end("compile", conclusion="success")
flush_file()
```

## Что окажется в таблице

Таблица по умолчанию — `analytics/events`. `start compile` запоминает время,
`end compile` считает длительность, `flush` пишет **одну строку**. Пока нет
`end`, в таблицу ничего не едет. `track` — сразу готовое число, без пары
start/end.

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

`flush` и `send` идемпотентны: повторно уже залитое не едет.

Флаги: `--kind duration\|gauge\|count\|event\|info`, `--value`, `--unit`,
`--duration-ms`, `--started-epoch`, `--finished-epoch`, `--conclusion`,
`--error`, `--label key=value` (повторяемый), `--run-id`, `--file`.

## Файлы рядом с буфером

| Файл | Зачем |
| --- | --- |
| `$ANALYTICS_FILE` | буфер, по строке на событие |
| `.offset` | сколько байт уже залито |
| `.pending` | незакрытые `start`; поэтому `flush` их не заливает |
| `.skipped` | отвергнутые строки + `reason` |
| `.lock` | flock на `enrich` / append |

Незакрытый хвост файла (обрыв записи) offset не перешагивает; следующий append
его завершает, чтобы не склеиться с новой строкой.

## Заливка в YDB

Креды — `ANALYTICS_YDB_CREDENTIALS` (путь к ключу). В этом репозитории ещё
читается `CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`.

Без кредов или без SDK `flush` печатает warning и выходит 0: процесс зелёный,
данных нет.

Таблица: PK `(event_ts, date, run_id, source, name, kind, span_id)`, TTL 1 год
на `event_ts`. `flush` создаёт её сам (`ensure_table=True`). Обёртка может
выключить это и создать таблицу отдельно.

Путь по умолчанию `analytics/events`. Логический ключ для
`ydb_wrapper.get_table_path` — `analytics_events`.

`YDBWrapper` сначала ищется обычным `import`, затем (только в этом репозитории)
по `../../../analytics/ydb_wrapper.py` относительно `collector/`. Класс должен
уметь:

```python
class YDBWrapper:
    def __enter__(self) -> "YDBWrapper": ...
    def __exit__(self, exc_type, exc, tb) -> bool: ...
    def check_credentials(self) -> bool: ...
    def get_table_path(self, table_name: str) -> str: ...
    def create_table(self, table_path: str, create_sql: str) -> None: ...
    def bulk_upsert_batches(self, table_path, rows, column_types, batch_size=1000) -> None: ...
```

Больше от него ничего не нужно.

## Тесты

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/collector -p 'test_*.py'
```
