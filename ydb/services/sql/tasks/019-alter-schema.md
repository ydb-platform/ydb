# ALTER SCHEMA отвечает 0A000

status: pending
commit:
id: alter-schema
depends: create-schema

## Сделать

`ALTER SCHEMA ... RENAME TO` и `OWNER TO`. У каталога нет операции переименования и смены владельца. Ответ `0A000`, propose не отправляется.

## Тест

Тест: имя каталога после `RENAME TO` прежнее.

## Готово, когда

Каталог не переименован и не сменил владельца.

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
