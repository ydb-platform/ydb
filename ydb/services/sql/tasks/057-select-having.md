# HAVING

status: pending
commit:
id: select-having
depends: select-group

## Сделать

Фильтр по результатам агрегации, тот же язык сравнений, что `WHERE`.

## Тест

Тест: группа, не прошедшая `HAVING`, отсутствует.

## Готово, когда

В ответе только группы, прошедшие фильтр.

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
