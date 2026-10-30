# WITH

status: done
commit: 52e2ad5d47cd2e4b67734af369c96ff8c19c2ab2
id: select-with
depends: select-subquery

## Сделать

`WITH` только у `SELECT`. Общее табличное выражение считается один раз и подставляется по имени.

## Тест

Тест: имя из `WITH` используется в основном `SELECT` дважды и даёт один и тот же набор.

## Готово, когда

Запрос с одним `WITH` выполняется.

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
