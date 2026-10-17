# ALTER TABLE ADD COLUMN

status: pending
commit:
id: alter-add-column
depends: create-table

## Сделать

`ALTER TABLE ... ADD COLUMN` через `ESchemeOpAlterTable`, список `Columns`. Имена типов те же, что у `CREATE TABLE`. Неподдержанный пункт — `0A000`, propose не отправляется.

## Тест

Тест: новый столбец виден в scheme cache. `ADD COLUMN` с `date` таблицу не меняет.

## Готово, когда

Добавлен только столбец с известным типом.

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
