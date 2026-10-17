# FOR UPDATE на row-таблице

status: pending
commit:
id: select-for-update
depends: tx-one-shard

## Сделать

`FOR UPDATE` / `FOR NO KEY UPDATE` / `FOR SHARE` / `FOR KEY SHARE` только для row-таблицы. Блокировка готовыми полями: `TEvRead.LockMode` (`PESSIMISTIC_EXCLUSIVE`, `PESSIMISTIC_EXCLUSIVE_NO_KEY`, `PESSIMISTIC_SHARED`, `PESSIMISTIC_SHARED_KEY`) либо `TEvLockRows`, включая `SkipLocked`. События шарда не менять. `NOWAIT` — `0A000`. Колоночная таблица — `0A000`.

## Тест

Тест: `SELECT ... FOR UPDATE` в транзакции блокирует строку так, что вторая транзакция не меняет её до `COMMIT` или `ROLLBACK`. `NOWAIT` — `0A000`.

## Готово, когда

Блокировка держится до конца транзакции и снимается при откате.

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
