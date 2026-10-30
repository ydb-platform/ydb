# CREATE SEQUENCE

status: done
commit: ad1bacceaae5d2748c6077708be8adbde994d7f1
id: create-sequence
depends: create-table

## Сделать

`CREATE SEQUENCE` через `ESchemeOpCreateSequence` и `TSequenceDescription`. Поля: `DataType` (`smallint`/`integer`/`bigint` → `Int16`/`Int32`/`Int64`, по умолчанию `bigint`), `Increment`, `MinValue`, `MaxValue`, `StartValue`, `Cache`, `Cycle`. `TEMPORARY`, `UNLOGGED`, `OWNED BY` — `0A000`, последовательность не создаётся. `nextval` не входит в коммит.

## Тест

Тест: последовательность видна в scheme cache с заданными границами. `TEMPORARY` ничего не создаёт.

## Готово, когда

В схеме есть последовательность с полями `TSequenceDescription`.

## Не трогать

- `ydb/core/tx/datashard`, `ydb/core/tx/columnshard` и их протобуфы
- `yql/` и `ydb/core/kqp`: ни `PEERDIR`, ни вызовы
- remote `official`
- код datashard и columnshard в одном читателе

Пока этой возможности нет в коде, она отвечает SQLSTATE `0A000`, текст `not supported`, сессия жива.
## Перед коммитом

```bash
./ya make --build relwithdebinfo -tA ydb/services/sql
```

Коммит только после зелёного прогона. В прогон входят тесты `pg8000`, когда они уже есть.

## После коммита

В этом файле поставить `status: done` и хеш в `commit`. Правка отметки входит в следующий коммит.
