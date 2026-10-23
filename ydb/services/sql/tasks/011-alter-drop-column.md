# ALTER TABLE DROP COLUMN

status: done
commit: a7d0bb736536fbfcb21ab902a1067f79ec8afacd
id: alter-drop-column
depends: alter-add-column

## Сделать

`ALTER TABLE ... DROP COLUMN` через `ESchemeOpAlterTable`, список `DropColumns`. Ключевой столбец, который schemeshard отказывается удалить, возвращает ошибку схемы, а не тихий успех.

## Тест

Тест: столбец исчезает из описания. Повторный DROP без `IF EXISTS` — ошибка.

## Готово, когда

Из описания таблицы удалён названный столбец.

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
