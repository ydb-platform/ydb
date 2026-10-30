# BEGIN, COMMIT, ROLLBACK на одном шарде

status: done
commit: a5808cd93e9c749038046a02eea489152d3fb2d1
id: tx-one-shard
depends: insert-row

## Сделать

`BEGIN` / `START TRANSACTION`, `COMMIT` / `END`, `ROLLBACK` / `ABORT` на одном шарде. Простой запрос без BEGIN коммитится сам. Один шард и нет незакрытых блокировок — `MODE_IMMEDIATE`. `ROLLBACK` не оставляет запись. После ошибки внутри явной транзакции сессия принимает только `ROLLBACK`, статус ReadyForQuery `E`, пока транзакция сломана; `I` вне транзакции, `T` внутри успешной. DDL внутри `BEGIN` — `0A000`.

## Тест

Тест отката: вставленная в транзакции строка после `ROLLBACK` не читается. После ошибки второй оператор до `ROLLBACK` — ошибка состояния транзакции.

## Готово, когда

Один шард либо фиксирует строку, либо не оставляет её.

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
