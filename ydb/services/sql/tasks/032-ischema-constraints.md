# Ограничения information_schema

status: done
commit: 8adbb02bac9b30f41f2f6a51789f270337c3cb28
id: ischema-constraints
depends: ischema-columns

## Сделать

Один коммит на `table_constraints` и `key_column_usage`. Первичный ключ показывается как `PRIMARY KEY` с именем `<таблица>_pkey`. Уникальный индекс — `UNIQUE` с именем индекса. Столбцы ограничения: catalog, schema, name, table_name, constraint_type. У usage ещё `column_name` и `ordinal_position`. `CHECK` и внешний ключ строк не имеют.

## Тест

Тест: PK и уникальный индекс видны, `CHECK` строки нет.

## Готово, когда

В ответе только ключ таблицы и уникальные индексы.

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
