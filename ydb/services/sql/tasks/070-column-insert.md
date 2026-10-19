# INSERT в колоночную таблицу

status: done
commit: 1d2c9e6de09dfd239e116708f6e75eea4bf8c72b
id: column-insert
depends: column-select

## Сделать

`TEvWrite` columnshard, arrow-батч, операция вставки. Код записи не делит файл с datashard. `BEGIN` на колоночной таблице — `0A000`, пока нет транзакций columnshard.

## Тест

Тест: вставленная строка читается `SELECT`.

## Готово, когда

Строка лежит в колоночной таблице.

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
