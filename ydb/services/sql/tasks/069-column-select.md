# SELECT по колоночной таблице

status: pending
commit:
id: column-select
depends: column-create

## Сделать

Каталог `columnshard`, свой исполнитель. `TEvRead` из `tx_columnshard.proto`: `ColumnIds`, `GreaterPredicate` / `LessPredicate`, `OlapProgram`. Фильтр, проекция, группировка и `DISTINCT`, которые помещаются в `TOlapProgram` (`ssa.proto`), считаются в columnshard. Программу собирает этот каталог, без YQL. `ORDER BY` и `LIMIT` в программе нет. Снимок берётся всегда. Остаток предиката — в сервисе. До этого коммита колоночная таблица в запросе данных — `0A000`. Не вызывать `TEvKqpScan`.

## Тест

Тест на мини-кластере с columnshard: `SELECT` по границе ключа возвращает строки. Предикат не по ключу, который вошёл в программу, не тянет лишние строки в сервис.

## Готово, когда

DataRow совпадают с записанными данными.

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
