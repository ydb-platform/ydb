## Логические операции

Для выполнения логических операций используются функции `PgAnd`, `PgOr`, `PgNot`:

```yql
SELECT
    PgAnd(PgBool(true), PgBool(true)), -- PgBool(true)
    PgOr(PgBool(false), null), -- PgCast(null, pgbool)
    PgNot(PgBool(true)), -- PgBool(false)
;
```
