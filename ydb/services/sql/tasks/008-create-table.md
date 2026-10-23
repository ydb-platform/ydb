# CREATE TABLE

status: done
commit: e7a8c0702de34772528838cd230f079457c071c4
id: create-table
depends: analyze-scheme

## Сделать

`CREATE TABLE` синтаксиса PostgreSQL через `TEvTxUserProxy::TEvProposeTransaction`, тело `TModifyScheme`, `ESchemeOpCreateTable`. Столбцы, `PRIMARY KEY` в `KeyColumnNames`, `NULL` / `NOT NULL`, `DEFAULT` литералом в `DefaultFromLiteral`, `IF NOT EXISTS` через `FailOnExist`.
Тип без кавычек — имя PostgreSQL: `int8`/`bigint` → `Int64`, `int4` → `Int32`, `int2` → `Int16`, `bool` → `Bool`, `float4` → `Float`, `float8` → `Double`, `text`/`varchar`/`char` → `Utf8`, `bytea` → `String`, `numeric(p,s)` → `Decimal(p,s)`, `numeric` без точности → `Decimal(22, 9)`, `uuid` → `Uuid`, `json` → `Json`, `jsonb` → `JsonDocument`, `timestamp` → `Timestamp64`, `timestamp(0)` → `Datetime64`, `interval` → `Interval64`. `date` → имя `Date64`, но такого типа в шарде нет: столбец `date` — `0A000`, propose не отправляется. Кавычки задают тип YDB как есть: `"Int8"`, `"Date"`, `"Date32"`.
`DyNumber` не использовать. Пункт без поля в `TTableDescription` или `TColumnDescription` — `0A000`, propose не отправляется: `PARTITION BY`, `INHERITS`, `TABLESPACE`, `WITH`, `ON COMMIT`, `COLLATE`, `CHECK`, `REFERENCES`, `LIKE`, смена типа, `RENAME`, `SET SCHEMA`. Дождаться завершения операции schemeshard до ReadyForQuery. Столбцы первичного ключа создаются `NOT NULL`.
Ждать завершения propose до ReadyForQuery.

## Тест

Тест pg8000 с autocommit: `int8` создаёт `Int64`, `"Int8"` — однобайтовый столбец, `timestamp` — `Timestamp64`. `CREATE TABLE ... PARTITION BY` и `CHECK` таблицу не создают. Таблица видна в scheme cache.

## Готово, когда

Схема меняется только когда каждый пункт команды имеет поле.

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
