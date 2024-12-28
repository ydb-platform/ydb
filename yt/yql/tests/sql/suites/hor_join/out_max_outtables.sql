/* postgres can not */
/* kikimr can not */
pragma yt.MaxOutputTables="3";
pragma yt.DisableOptimizers="HorizontalJoin,MultiHorizontalJoin";

SELECT * FROM (
    SELECT CAST(key AS int) as key, '' as value FROM plato.Input
    UNION ALL
    SELECT 0 as key, value from plato.Input
    UNION ALL
    SELECT 1 as key, value from plato.Input
    UNION ALL
    SELECT 2 as key, value from plato.Input
    UNION ALL
    SELECT 3 as key, value from plato.Input
) AS x
ORDER BY key, value
;
