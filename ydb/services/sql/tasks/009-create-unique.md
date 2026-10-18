# UNIQUE в CREATE TABLE

status: done
commit: 1cd6371954d4cb763b01b5476a83ef345dd11513
id: create-unique
depends: create-table

## Сделать

`UNIQUE (...)` в `CREATE TABLE` создаёт индекс `EIndexTypeGlobalUnique` в `TableIndexes`. Это отдельная возможность от обычного `CREATE TABLE`.

## Тест

Тест: таблица с `UNIQUE` видна в scheme cache как уникальный синхронный индекс. Проверка `23505` — после коммита вставки, здесь только схема.

## Готово, когда

В описании таблицы есть уникальный синхронный индекс.

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
