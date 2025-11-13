# ALTER STREAMING QUERY

<!-- markdownlint-disable proper-names -->

Вызов `ALTER STREAMING QUERY` изменяет настройки или текст [стриминговых запросов](../../../concepts/streaming_query/index.md), а также управляет состоянием запроса (остановкой/запуском).

```sql
ALTER STREAMING QUERY [IF EXISTS] <query name> [SET (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] [AS
DO BEGIN
    <query statement1>;
    <query statement2>;
    ...
END DO];
```

Настройки SET:

- `RUN = (TRUE|FALSE)` - запустить или остановить запрос.
Статус запроса (запущен или остановлен) не изменяется, если явно не указывать настройку `RUN`.
- `FORCE = (TRUE|FALSE)`  нужно ли разрешать изменение запроса приводящее к невозможности его загрузки из чекпоинта, по умолчанию FALSE.
Примеры:

```sql
-- Stop query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = FALSE
);

-- Start created query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = TRUE
);

-- Change query text
ALTER STREAMING QUERY `my_queries/query_name` SET (
    FORCE = TRUE -- Allow to drop checkpoint in case of incompatible changes in query.
) AS DO BEGIN
PRAGMA FeatureR010 = 'prototype';

$input = (
    SELECT
        *
    FROM
        `source_name`.`input_topic_name` WITH (
            FORMAT = 'json_each_row',
            SCHEMA (time String NOT NULL, event_class String NOT NULL, host String NOT NULL, message String NOT NULL)
        )
);

$filtered = (
    SELECT
        *
    FROM
        $input
    WHERE
        event_class == 'login'
);

$matches = (
    SELECT
        *
    FROM
        $filtered MATCH_RECOGNIZE (
            MEASURES
                FIRST(F1.time) AS time,
                FIRST(F1.host) AS host
            ONE ROW PER MATCH
            PATTERN (F1 F2)
            DEFINE
                F1 AS F1.message == 'login failed',
                F2 AS F2.message == 'login failed' AND F2.host == FIRST(F1.host)
        )
);

$json = (
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        $matches
);

INSERT INTO `source_name`.`output_topic_name`
SELECT
    *
FROM
    $json
;
END DO;


-- Change and start query
ALTER STREAMING QUERY `my_queries/query_name` SET (
    RUN = TRUE
) AS
DO BEGIN
    ...
END DO;
```

