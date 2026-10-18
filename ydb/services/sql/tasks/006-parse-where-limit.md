# Разбор WHERE и LIMIT

status: done
commit: 67118e418861875f2dd9018e7273e6becf99616c
id: parse-where-limit
depends: parse-select

## Сделать

Добавить в грамматику `WHERE` из сравнений и `AND` и необязательный `LIMIT`. Другие логические связки, которых ещё нет, остаются `0A000`.

## Тест

Юнит: дерево `WHERE` и `LIMIT`. `OR` в этом коммите — `0A000`, если он ещё не разобран как остаточный предикат.

## Готово, когда

`WHERE` и `LIMIT` есть в дереве, сессия на них не падает.

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
