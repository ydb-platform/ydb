# information_schema.columns

status: pending
commit:
id: ischema-columns
depends: ischema-tables

## Сделать

Столбцы: `table_catalog`, `table_schema`, `table_name`, `column_name`, `ordinal_position`, `data_type`, `is_nullable`, `column_default`. `data_type` — имя PostgreSQL из таблицы соответствия. `column_default` — текст литерала. Значение из последовательности даёт пустой default. Describe только тех таблиц, чьи столбцы нужны запросу.

## Тест

Тест: столбец `bigint` виден как `int8`, `NOT NULL` и литеральный default совпадают со схемой.

## Готово, когда

Список столбцов совпадает с описанием таблицы.

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
