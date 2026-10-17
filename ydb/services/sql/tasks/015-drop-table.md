# DROP TABLE

status: pending
commit:
id: drop-table
depends: create-table

## Сделать

`DROP TABLE` через `ESchemeOpDropTable`, тело `TDrop`. `IF EXISTS` при отсутствии пути завершает команду без ошибки. Дождаться завершения propose.

## Тест

Тест: таблица исчезает из scheme cache. `DROP TABLE IF EXISTS` отсутствующего пути — успех.

## Готово, когда

Путь удалён либо команда честно сообщила, что его нет.

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
