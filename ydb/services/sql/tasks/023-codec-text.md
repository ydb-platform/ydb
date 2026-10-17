# Кодек text и bytea

status: pending
commit:
id: codec-text
depends: codec-ints

## Сделать

`Utf8` → text, `String` / `String4k` / `String2m` → bytea. `varchar(n)` и `char(n)` не обрезаются и не дополняются пробелами.

## Тест

Юнит: текст UTF-8 и байты, в том числе длиннее типичного varchar.

## Готово, когда

Байты и текст проходят туда и обратно без подгонки длины.

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
