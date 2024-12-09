/* postgres can not */
/* kikimr can not */
pragma yt.MaxInputTables="3";

SELECT * FROM (
    SELECT CAST(key AS int) as key, '' as value FROM plato.Input1
    UNION ALL
    SELECT 0 as key, value from plato.Input2
    UNION ALL
    SELECT 1 as key, value from plato.Input3
    UNION ALL
    SELECT 2 as key, value from plato.Input4
    UNION ALL
    SELECT 3 as key, value from plato.Input5
) AS x
ORDER BY key, value
;
