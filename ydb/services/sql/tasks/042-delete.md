# DELETE одной таблицы

status: done
commit: 4280412b51caa7d0b715f92846fdbfa7d0ec6de3
id: delete
depends: update

## Сделать

Одна таблица, `WHERE` как у `SELECT`, `OPERATION_DELETE`. Те же ограничения индекса, что у `UPDATE`. `USING` другой таблицы — `0A000`.

## Тест

Тест: строка по `WHERE` исчезает. Таблица с синхронным индексом — `0A000`, строка на месте.

## Готово, когда

Удалены только строки таблицы без синхронного индекса.

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
