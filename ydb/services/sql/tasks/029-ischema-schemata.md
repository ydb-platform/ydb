# information_schema.schemata

status: done
commit: aebd1daae38db0b2dddc58be001cfb2842fdcfc8
id: ischema-schemata
depends: select-where-limit

## Сделать

Виртуальное отношение, не каталог и не чтение шарда. Столбцы: `catalog_name`, `schema_name` — прямые каталоги корня базы. `schema_owner` только если выбранный источник уже содержит владельца. Источник любой готовый, scheme cache не обязателен. Запись — `0A000`. Если `schema_name` в `WHERE` — равенство константе, читается один каталог.

## Тест

Тест: после `CREATE SCHEMA` имя есть в `schemata`. Лишний столбец — `0A000`.

## Готово, когда

В ответе только созданные каталоги первого уровня.

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
