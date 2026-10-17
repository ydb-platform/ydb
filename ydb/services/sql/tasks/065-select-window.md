# Оконные функции OVER

status: pending
commit:
id: select-window
depends: select-order

## Сделать

Окно `OVER` с `PARTITION BY` и `ORDER BY` считается в сервисе. Набор функций среза: `row_number`, `rank`. Остальные окна — `0A000`, пока нет своего расширения этого коммита. На шард уходит только чтение.

## Тест

Тест: `row_number()` нумерует строки раздела.

## Готово, когда

Номера в разделе идут с 1 без дыр.

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
