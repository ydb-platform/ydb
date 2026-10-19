# OFFSET и FETCH

status: done
commit: 3be7b80c7cbf2213ae170bff36179918376cedce
id: select-offset
depends: select-where-limit

## Сделать

`OFFSET` и `FETCH` после сортировки, если она есть, иначе после чтения. На row-datashard `LIMIT` программы нет, отбрасывание в сервисе.

## Тест

Тест: `OFFSET 1 FETCH FIRST 1 ROW ONLY` возвращает одну строку не первую.

## Готово, когда

Пропущены и оставлены ровно запрошенные строки.

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
