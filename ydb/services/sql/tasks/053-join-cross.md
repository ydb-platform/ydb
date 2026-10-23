# CROSS JOIN

status: done
commit: 18fb5d395d6aea7a1620732a901dcbbdfc195b00
id: join-cross
depends: join-inner

## Сделать

Произведение входов без предиката. Запись `FROM a, b` разбирается здесь же. Оба входа читаются один раз.

## Тест

Тест: число строк равно произведению размеров входов.

## Готово, когда

Ответ — декартово произведение.

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
