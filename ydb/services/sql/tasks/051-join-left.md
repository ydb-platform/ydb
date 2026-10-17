# LEFT JOIN

status: pending
commit:
id: join-left
depends: join-inner

## Сделать

Сохраняет строки левого входа. Для несовпадения правые столбцы — `NULL`.

## Тест

Тест: левая строка без пары присутствует, правые столбцы пустые.

## Готово, когда

Все левые строки есть в ответе.

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
