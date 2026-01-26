# Запись в таблицы 

Запись результата в таблицу {{ ydb-short-name }} возможна с помощью [UPSERT INTO](../../yql/reference/syntax/upsert_into).

```yql
CREATE STREAMING QUERY query_with_table_write AS
DO BEGIN

UPSERT INTO
    output_table
SELECT
    Unwrap(CAST(Ts AS Timestamp)) AS Ts,
    Country,
    Count
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Ts String NOT NULL,
        Count Uint64 NOT NULL,
        Country Utf8 NOT NULL
    )
)

END DO
```

{% note alert %}

Не поддерживаются:
* команда [INSERT INTO](../../yql/reference/syntax/insert_into);
* запись в таблицы {{ ydb-short-name }}, находящихся во внешних БД

{% endnote %}