# SET и SHOW

status: pending
commit:
id: set-show
depends: wire-startup

## Сделать

Меняются и показываются `DateStyle`, `TimeZone`, `standard_conforming_strings`. `SHOW client_encoding` возвращает `UTF8`. `SET client_encoding` в другое значение — `0A000`, кодировка остаётся UTF-8. Успешный `SET` известного параметра шлёт ParameterStatus. Неизвестный параметр, `SET TRANSACTION` и `search_path` — `0A000`.

## Тест

Тест: `SHOW` возвращает значение после `SET`. `SET client_encoding TO 'LATIN1'` — `0A000`, следующий `SHOW` — `UTF8`.

## Готово, когда

Сессия хранит только уже объявленные параметры и всегда UTF-8.

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
