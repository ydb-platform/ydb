# RETURNING у UPDATE и DELETE

status: pending
commit:
id: update-delete-returning
depends: delete

## Сделать

Тот же протокол, что у `INSERT RETURNING`: RowDescription, DataRow, CommandComplete. Для `DELETE` в DataRow строка до удаления. Выражение сложнее столбца — `0A000`.

## Тест

Тест: `UPDATE ... RETURNING` показывает новое значение, `DELETE ... RETURNING` — удалённую строку.

## Готово, когда

Клиент получает затронутые строки.

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
