/* postgres can not */
/* kikimr can not */

SELECT * FROM (
    SELECT key, value || "a" as value FROM plato.Input
    union all
    SELECT key, "1" as value from plato.Input
    union all
    SELECT key, "1" as value from plato.Input
    union all
    SELECT key, "3" as value from plato.Input
) AS x
ORDER BY key, value
;
