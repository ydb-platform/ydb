# DISTINCT и DISTINCT ON

status: done
commit: ad23553fe390ec933b41d5beeefead08908be5da
id: select-distinct
depends: select-where-limit

## Сделать

`DISTINCT` и `DISTINCT ON`. Для колоночной таблицы `Distinct` из `TOlapProgram` можно отдать в columnshard, когда колоночный путь уже есть. До того — в сервисе для row-таблицы.

## Тест

Тест: повторы исчезают. `DISTINCT ON` оставляет первую строку группы в заданном порядке, если `ORDER BY` уже есть; иначе порядок внутри группы не обещать сверх того, что прочитано.

## Готово, когда

В ответе нет повторов по заданным столбцам.

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
