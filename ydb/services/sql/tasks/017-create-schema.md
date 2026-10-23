# CREATE SCHEMA

status: done
commit: 59ca6396180674c9461f671ceddaaa459a1278cb
id: create-schema
depends: create-table

## Сделать

Схема — один каталог непосредственно под корнем базы. `CREATE SCHEMA app` — `ESchemeOpMkDir`, `TMkDir`, `WorkingDir` корень базы. Без `IF NOT EXISTS` — `FailOnExist`. `AUTHORIZATION` и объекты в теле команды — `0A000`, каталог не создаётся. `information_schema` и `pg_catalog` каталогами не создавать.

## Тест

Тест: каталог виден в scheme cache. `CREATE SCHEMA s CREATE TABLE t (...)` каталог не создаёт.

## Готово, когда

Появился только пустой каталог первого уровня.

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
