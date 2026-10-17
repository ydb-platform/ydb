# CREATE INDEX и CREATE UNIQUE INDEX

status: pending
commit:
id: create-index
depends: create-table

## Сделать

`ESchemeOpCreateTableIndex` и `TIndexCreationConfig`. Обычный индекс — `EIndexTypeGlobal`, уникальный — `EIndexTypeGlobalUnique`. `INCLUDE` пишет покрывающие столбцы в `DataColumnNames`. `CONCURRENTLY`, `TABLESPACE`, `USING` и `WHERE` — `0A000`, индекс не создаётся. Асинхронный индекс этой командой не создаётся.

## Тест

Тест: синхронный и уникальный индексы видны в scheme cache. `CREATE INDEX ... WHERE` индекс не создаёт.

## Готово, когда

Создан только синхронный или уникальный синхронный индекс без предиката.

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
