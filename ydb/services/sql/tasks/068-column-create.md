# CREATE TABLE колоночной таблицы

status: done
commit: e2a9dccc8705ab3d200e2e216c6d70a7cda38eeb
id: column-create
depends: create-table

## Сделать

`CREATE TABLE ... WITH (store = column)` — `ESchemeOpCreateColumnTable`. Другие ключи `WITH` — `0A000`, таблица не создаётся. Без этого ключа по-прежнему row-таблица. `ALTER TABLE` существующей колоночной таблицы — `ESchemeOpAlterColumnTable` для добавления и удаления столбца. Код DDL колоночной таблицы не смешивать с row-путём одним `if` внутри общего построителя: стратегия на вид таблицы.

## Тест

Тест: `WITH (store = column)` создаёт колоночную таблицу. `WITH (fillfactor = 10)` не создаёт ничего. `Float` в первичном ключе колоночной таблицы — отказ схемы или `0A000` до propose, если тип ключу запрещён.

## Готово, когда

В scheme cache таблица колоночная, другие параметры WITH её не создают.

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
