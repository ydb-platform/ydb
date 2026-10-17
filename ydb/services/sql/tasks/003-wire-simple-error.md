# Simple query отвечает 0A000

status: done
commit: 0b1adb6efb4e2586fff2694eecc0a81623886f4c
id: wire-simple-error
depends: wire-startup

## Сделать

Сообщение `Q` отвечает ErrorResponse SQLSTATE `0A000`, текст `not supported`, затем снова ReadyForQuery. Cancel и SSL request не роняют слушатель: на SSL request уходит байт `N`. Extended (`P`/`B`/`D`/`E`/`S`) до своего коммита тоже `0A000`.

## Тест

Тест: любой simple query получает `0A000` и ReadyForQuery. SSL request и Cancel оставляют слушатель живым.

## Готово, когда

Сессия после неподдержанного запроса принимает следующий.

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
