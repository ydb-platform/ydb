# EXPLAIN

status: pending
commit:
id: explain
depends: select-where-limit

## Сделать

`EXPLAIN` уже разобранного оператора. Ответ — один столбец `QUERY PLAN`, text. Оператор не выполняется: шард не читается и не пишется, схема не меняется. Для `SELECT` план называет форму `Keys` или `Ranges`, число шардов, остаток предиката в сервисе и `LIMIT`, если он есть. Команда, которая сама отвечает `0A000`, и `EXPLAIN ANALYZE` отвечают `0A000`.

## Тест

Тест через сессию, без сокета: `EXPLAIN` точечного `SELECT` содержит `Keys` и один шард и не меняет таблицу. `EXPLAIN ANALYZE` — `0A000`.

## Готово, когда

Текст плана описывает выбранный путь, а данные таблицы те же, что до команды.

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

Коммит только после зелёного прогона. Тест команды идёт через `TSqlClient`, без кадра протокола.

## После коммита

В этом файле поставить `status: done` и хеш в `commit`. Правка отметки входит в следующий коммит.
