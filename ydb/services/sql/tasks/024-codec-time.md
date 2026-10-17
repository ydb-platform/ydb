# Кодек даты, timestamp и interval

status: pending
commit:
id: codec-time
depends: codec-text

## Сделать

Существующие типы: `Date`, `Date32`, `Datetime`, `Datetime64`, `Timestamp`, `Timestamp64`, `Interval`, `Interval64`. На проводе date, timestamp и interval. Месяцы и дни интервала PostgreSQL отвергаются. `Date64` в кодек не добавлять: типа нет. Часового пояса нет.

## Тест

Юнит границ: 1970-01-01, конец диапазона `Date`/`Timestamp`, дата до 1970 для `Date32`, широкие 64-битные типы.

## Готово, когда

Значение внутри диапазона типа шарда сохраняется, значение вне диапазона — ошибка до записи.

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
