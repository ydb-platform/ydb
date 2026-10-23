# RIGHT JOIN

status: done
commit: f2f7d39d568092568ef1a7f3ad90c0ac522afb4f
id: join-right
depends: join-left

## Сделать

Сохраняет строки правого входа. Планировщик может поменять входы и свести к левому соединению.

## Тест

Тест: правая строка без пары присутствует.

## Готово, когда

Все правые строки есть в ответе.

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
