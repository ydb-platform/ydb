/* postgres can not */
/* kikimr can not */
PRAGMA yt.MaxInputTables = "3";

SELECT
    *
FROM (
    SELECT
        CAST(key AS int) AS key,
        '' AS value
    FROM
        plato.Input1
    UNION ALL
    SELECT
        0 AS key,
        value
    FROM
        plato.Input2
    UNION ALL
    SELECT
        1 AS key,
        value
    FROM
        plato.Input3
    UNION ALL
    SELECT
        2 AS key,
        value
    FROM
        plato.Input4
    UNION ALL
    SELECT
        3 AS key,
        value
    FROM
        plato.Input5
) AS x
ORDER BY
    key,
    value
;
