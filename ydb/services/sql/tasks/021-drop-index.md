# DROP INDEX

status: done
commit: 2fdfe5a0eff90f14f6750d7eeb4e343e38a01014
id: drop-index
depends: create-index

## Сделать

Путь индекса находится через scheme cache, затем `ESchemeOpDropTableIndex` и `TDropIndex`.

## Тест

Тест: индекс исчезает из описания таблицы, таблица остаётся.

## Готово, когда

Удалён названный индекс.

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
