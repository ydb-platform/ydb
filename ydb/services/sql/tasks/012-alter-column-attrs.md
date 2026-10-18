# ALTER COLUMN NOT NULL и DEFAULT

status: done
commit: 2e9fb9bd9b0c59ded590fef430f637008bc8cb9e
id: alter-column-attrs
depends: alter-drop-column

## Сделать

`ALTER COLUMN SET NOT NULL`, `DROP NOT NULL`, `SET DEFAULT`, `DROP DEFAULT`: поля `NotNull` и значение по умолчанию столбца. Смена типа — `0A000`, propose не отправляется.

## Тест

Тест: `NOT NULL` и литеральный `DEFAULT` видны в описании столбца. `ALTER COLUMN TYPE` описание не меняет.

## Готово, когда

Меняются только nullability и константный default.

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
