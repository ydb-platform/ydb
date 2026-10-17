# ALTER SEQUENCE

status: pending
commit:
id: alter-sequence
depends: create-sequence

## Сделать

`ALTER SEQUENCE` через `ESchemeOpAlterSequence`. Меняет тип среди трёх целых и делает `RESTART` через `Restart` и `SetVal`. Границы проверяет уже schemeshard. `OWNED BY` — `0A000`, последовательность не меняется.

## Тест

Тест: `RESTART` меняет описание. `OWNED BY` оставляет прежние поля.

## Готово, когда

Изменяются только поля, которые есть в `TSequenceDescription`.

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
