# collector

JSONL-буфер и заливка в YDB. Про GitHub ничего не знает: в другой проект
копируется эта папка целиком.

## Модель

- `start` / `end` — открыть и закрыть span, длительность считается по разнице.
- `track` — записать уже готовое событие, span не открывается.
- `enrich` — дописать labels в последнюю неотправленную строку с этим именем.
  Длительность, `event_ts` и `conclusion` не меняет, поэтому ссылку на отчёт
  можно приклеить после измерения.
- `flush` — залить в YDB только закрытые строки.
- `send` — закрыть незакрытые span и залить всё.

`flush` и `send` идемпотентны: заливать можно сколько угодно раз, отправленное
не поедет второй раз.

## Обязательные поля

Строка без любого из этих полей молча не доедет до YDB — попадёт в
`$CI_METRICS_FILE.skipped` с причиной:

| Поле | Откуда берётся |
| --- | --- |
| `name` | первый аргумент команды |
| `source` | `--source`; без него строка отбрасывается |
| `run_id` | `--run-id` или `$ANALYTICS_RUN_ID` |
| `event_ts` | ставится автоматически при `start` / `track` |
| `span_id` | генерируется автоматически |

То есть на практике надо помнить про `--source` и `ANALYTICS_RUN_ID`, остальное
collector заполняет сам.

## CLI

```bash
export PYTHONPATH=/path/to/analytics   # каталог, в котором лежит collector/
export CI_METRICS_FILE=/tmp/ci_metrics.jsonl
export ANALYTICS_RUN_ID=42

python3 -m collector start my_step --source my_job --label k=v
python3 -m collector end my_step --conclusion success
python3 -m collector enrich my_step --label report_url="$URL"
python3 -m collector track my_gauge --kind gauge --unit bytes --value 123 --source my_job
python3 -m collector flush
python3 -m collector send --conclusion cancelled   # если остались открытые span
```

`--file` перекрывает `$CI_METRICS_FILE`.

Остальные флаги: `--kind duration|gauge|count|event|info`, `--value`, `--unit`,
`--duration-ms`, `--started-epoch`, `--finished-epoch`, `--conclusion`,
`--error`, `--label key=value` (можно несколько), `--run-id`.

## Python

```python
from collector import start, end, track, enrich, flush_file, send

start("my_step", source="my_job", labels={"k": "v"})
end("my_step", conclusion="success")
enrich("my_step", labels={"report_url": url})
track("my_gauge", kind="gauge", unit="bytes", value=123, source="my_job")
flush_file()
```

## Файлы на диске

Рядом с `$CI_METRICS_FILE` появляются ещё четыре:

| Файл | Зачем |
| --- | --- |
| `$CI_METRICS_FILE` | сам буфер, по строке на событие |
| `.offset` | сколько байт уже залито; поэтому повторный `flush` не дублирует |
| `.pending` | открытые span; именно из-за него `flush` пишет только закрытые строки |
| `.skipped` | строки, которые YDB не примет, с полем `reason` |
| `.lock` | flock, чтобы `enrich` и параллельный append не потеряли запись |

Если складываете артефакты или чистите temp — это весь список. Кладите буфер
туда, откуда он не уедет в публичный бакет.

Незакрытая строка (обрыв записи) остаётся в буфере: offset до неё не двигается,
а следующий append её завершает, чтобы не склеиться с ней и не испортить и свою
запись тоже.

## Таблица

По умолчанию `analytics/events`, ключ в `ydb_qa_config.json` —
`analytics_events`.

PK `(event_ts, date, run_id, source, name, kind, span_id)`, TTL 1 год на
`event_ts`. Колонка под TTL должна быть первой в PK. `flush` создаёт таблицу сам
(`ensure_table=True`); обёртки могут это отключить и создавать её отдельным
шагом.

## Что нужно для заливки

Креды — одна из переменных:

- `ANALYTICS_YDB_CREDENTIALS`
- `CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`

Плюс SDK `ydb` и модуль `ydb_wrapper`. Без них `flush` печатает warning и
выходит с кодом 0 — то есть сборка зелёная, а данных нет. Если переносите
collector в другой проект, это первое, на что стоит посмотреть.

`ydb_wrapper` ищется так: сначала обычным `import ydb_wrapper` из
`PYTHONPATH`, потом по пути `../../../analytics/ydb_wrapper.py` относительно
`collector/` (в этом репозитории это `.github/scripts/analytics/ydb_wrapper.py`).

Контракт, который должен реализовать класс `YDBWrapper`:

```python
class YDBWrapper:
    def __enter__(self) -> "YDBWrapper": ...
    def __exit__(self, exc_type, exc, tb) -> bool: ...

    def check_credentials(self) -> bool:
        """False — кредов нет; flush тихо оставит батч на диске."""

    def get_table_path(self, table_name: str) -> str:
        """Логический ключ -> путь таблицы. KeyError, если ключа нет."""

    def create_table(self, table_path: str, create_sql: str) -> None: ...

    def bulk_upsert_batches(
        self,
        table_path: str,
        rows: list[dict],
        column_types: "ydb.BulkUpsertColumns",
        batch_size: int = 1000,
    ) -> None: ...
```

Больше от него ничего не требуется.

## Тесты

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/collector -p 'test_*.py'
```
