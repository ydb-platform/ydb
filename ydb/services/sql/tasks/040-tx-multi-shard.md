# Запись на несколько шардов

status: pending
commit:
id: tx-multi-shard
depends: tx-one-shard

## Сделать

Несколько шардов: `MODE_VOLATILE_PREPARE` на каждого участника, затем `TKqpLocks.Op = Commit`, `SendingShards`, `ReceivingShards`. При пяти и более пишущих шардах — `ArbiterShard`. `ROLLBACK` — `Op = Rollback`. Повтор одного участника после prepare остальных не делается: транзакция откатывается. `INSERT` нескольких строк группируется по шарду. Чтение внутри явной транзакции берёт блокировки и участвует в том же commit.

## Тест

Тест: две строки `INSERT` на два шарда видны одним `SELECT`. `ROLLBACK` не оставляет ни одной.

## Готово, когда

Обе строки появляются вместе либо не появляется ни одна.

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
