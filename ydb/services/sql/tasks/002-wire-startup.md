# Startup до ReadyForQuery

status: done
commit: b9b6ff9dfe11acd4bdad7c467fb8820271eff030
id: wire-startup
depends: scaffold

## Сделать

Сессия протокола 3.0: StartupMessage, AuthenticationOk (trust), ParameterStatus (`server_version`, `client_encoding=UTF8`, `DateStyle`, `integer_datetimes`, `standard_conforming_strings`), BackendKeyData, ReadyForQuery.
Параметр `database` строки подключения — путь базы YDB. Пустой или неизвестный путь не доводит сессию до ReadyForQuery. Кодировка всегда UTF-8.

## Тест

Тест доходит до ReadyForQuery с непустым путём базы. Пустой `database` ReadyForQuery не получает.

## Готово, когда

Рукопожатие завершается ReadyForQuery только при известном пути базы.

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
