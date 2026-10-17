# information_schema.tables

status: pending
commit:
id: ischema-tables
depends: ischema-schemata

## Сделать

Столбцы: `table_catalog`, `table_schema`, `table_name`, `table_type`. Строки `KindTable` и `KindColumnTable`, тип `BASE TABLE`. Объект в корне базы имеет пустой `table_schema`. Индексы, последовательности и прочие виды не входят.

## Тест

Тест: созданная таблица видна, последовательность не видна.

## Готово, когда

В отношении только таблицы.

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
