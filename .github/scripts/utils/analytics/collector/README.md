# collector

JSONL-буфер и flush в YDB. В другой проект копируется эта папка.

`start` / `end` считают длительность. `track` пишет уже готовое событие. `enrich` дописывает labels в последнюю неотправленную строку с этим именем (длительность не меняет). `flush` записывает в YDB только закрытые строки. `send` закрывает незакрытые span и записывает всё.

## CLI

```bash
export PYTHONPATH=/path/to/analytics   # каталог, в котором лежит collector/
export CI_METRICS_FILE=/tmp/ci_metrics.jsonl
export ANALYTICS_RUN_ID=42             # обязателен: без run_id строка не уйдёт в YDB

python3 -m collector start my_step --source my_job --label k=v
python3 -m collector end my_step --conclusion success
python3 -m collector enrich my_step --label report_url="$URL"
python3 -m collector track my_gauge --kind gauge --unit bytes --value 123 --source my_job
python3 -m collector flush
python3 -m collector send --conclusion cancelled   # если остались открытые span
```

`--file` перекрывает `$CI_METRICS_FILE`.

Полезные флаги: `--kind duration|gauge|count|event|info`, `--source`, `--value`, `--unit`, `--duration-ms`, `--started-epoch`, `--finished-epoch`, `--conclusion`, `--error`, `--label key=value` (можно несколько), `--run-id`.

## Python

```python
from collector import start, end, track, enrich, flush_file, send

start("my_step", source="my_job", labels={"k": "v"})
end("my_step", conclusion="success")
enrich("my_step", labels={"report_url": url})
track("my_gauge", kind="gauge", unit="bytes", value=123, source="my_job")
flush_file()
```

## Таблица

По умолчанию: `analytics/events`. Ключ в `ydb_qa_config.json`: `analytics_events`.

PK: `(event_ts, date, run_id, source, name, kind, span_id)`. `span_id` обязателен; collector сам генерирует его при `start`/`track`. TTL — 1 год на `event_ts` (колонка TTL должна быть первой в PK).

Невалидные строки после успешного upsert пишутся в `$CI_METRICS_FILE.skipped` (`reason` + исходная запись), offset всё равно двигается. Если валидных нет — offset не трогаем.

Для `flush` / `send` нужны SDK `ydb` и `ydb_wrapper` (в этом репозитории — `.github/scripts/analytics/ydb_wrapper.py`, в `PYTHONPATH` или рядом) плюс один из:

- `ANALYTICS_YDB_CREDENTIALS`
- `CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`

В этом репозитории `ydb_wrapper` живёт в `.github/scripts/analytics`. Без него flush только пишет warning и выходит 0.

```bash
python3 -m unittest discover -s .github/scripts/utils/tests/analytics/collector -p 'test_*.py'
```
