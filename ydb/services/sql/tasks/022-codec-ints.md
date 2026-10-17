# Кодек целых и bool

status: pending
commit:
id: codec-ints
depends: analyze-scheme

## Сделать

Кодек переводит уже хранимый тип столбца в формат протокола и обратно. Этот коммит: `Bool`, `Int8`, `Int16`, `Int32`, `Int64`, `Uint8`, `Uint16`, `Uint32`, `Uint64`. На проводе: bool, int2, int4, int8, для `Uint64` — numeric. Text и binary. `NULL` — пустая ячейка, не отдельный тип. Вызов на ячейку прямой, без виртуальной функции на каждую ячейку.

## Тест

Юнит text и binary на границах диапазонов, включая `Uint64` выше 2^63−1.

## Готово, когда

Круг кодирования сохраняет значение внутри диапазона типа.

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
