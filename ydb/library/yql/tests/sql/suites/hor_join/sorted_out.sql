/* postgres can not */
/* kikimr can not */
pragma yt.DisableOptimizers="UnorderedOuts";

SELECT * FROM (
    SELECT key, value || "a" as value FROM plato.Input1
    union all
    SELECT key, "1" as value from plato.Input2
    union all
    SELECT key, "2" as value from plato.Input3
    union all
    SELECT key, "3" as value from plato.Input4
    union all
    SELECT key, "4" as value from plato.Input5
) AS x
ORDER BY key, value
;