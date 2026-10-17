# INSERT одной строки

status: pending
commit:
id: insert-row
depends: select-point

## Сделать

Одна строка, `TEvWrite` `MODE_IMMEDIATE` `OPERATION_INSERT`, шард по первичному ключу. Повтор ключа — `23505`, строка не меняется. CommandComplete `INSERT 0 1`. Таблица с синхронным индексом `Ready` или `WriteOnly` — `0A000`. Таблица только с асинхронным индексом принимается. Несколько строк на разные шарды в этом коммите — `0A000`. `INSERT ... SELECT` — `0A000`.

## Тест

Тест: строка читается следующим `SELECT`. Повтор ключа — `23505` и старое значение на месте.

## Готово, когда

Вставлена одна новая строка либо честно отвергнут дубликат.

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
