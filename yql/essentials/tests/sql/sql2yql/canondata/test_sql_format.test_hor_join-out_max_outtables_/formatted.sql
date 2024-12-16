/* postgres can not */
/* kikimr can not */
PRAGMA yt.MaxOutputTables = "3";
PRAGMA yt.DisableOptimizers = "HorizontalJoin,MultiHorizontalJoin";

SELECT
    *
FROM (
    SELECT
        CAST(key AS int) AS key,
        '' AS value
    FROM
        plato.Input
    UNION ALL
    SELECT
        0 AS key,
        value
    FROM
        plato.Input
    UNION ALL
    SELECT
        1 AS key,
        value
    FROM
        plato.Input
    UNION ALL
    SELECT
        2 AS key,
        value
    FROM
        plato.Input
    UNION ALL
    SELECT
        3 AS key,
        value
    FROM
        plato.Input
) AS x
ORDER BY
    key,
    value
;
