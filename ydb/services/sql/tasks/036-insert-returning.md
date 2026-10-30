# INSERT RETURNING

status: done
commit: a399f17da01df1d734c1491b995790414f05062c
id: insert-returning
depends: insert-on-conflict

## Сделать

RowDescription, DataRow затронутых строк, CommandComplete `INSERT 0 n`. В образ входят заданные значения; прочие столбцы — `NULL` или константный default. Выражение по умолчанию и выражение в `RETURNING` сложнее столбца — `0A000`. `DO NOTHING` не шлёт DataRow. Имя в RowDescription — метка `AS`, иначе имя столбца.

## Тест

Тест: `RETURNING` одной вставленной строки и `RETURNING` после `DO UPDATE`. `DO NOTHING` без DataRow.

## Готово, когда

Клиент видит затронутые строки тем же протоколом, что `SELECT`.

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
