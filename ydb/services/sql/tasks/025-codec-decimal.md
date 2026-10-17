# Кодек decimal, uuid и json

status: pending
commit:
id: codec-decimal
depends: codec-time

## Сделать

`Decimal(p,s)` ↔ numeric(p,s), p 1..35. NaN и Infinity не представляются. `Uuid` ↔ uuid. `Json` ↔ json текстом. `JsonDocument` ↔ jsonb текстом JSON, двоичный jsonb PostgreSQL не воспроизводится. `DyNumber` не кодековать: столбец этого типа — `0A000`.

## Тест

Юнит text и binary для decimal, uuid и json. `DyNumber` даёт `0A000`.

## Готово, когда

Три семейства проходят круг кодирования, `DyNumber` не читается.

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
