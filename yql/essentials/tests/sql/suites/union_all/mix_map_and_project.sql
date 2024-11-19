/* postgres can not */
/* kikimr can not */
PRAGMA yt.InferSchema;

SELECT * FROM (
    SELECT key, '' as value FROM plato.Input
    UNION ALL
    SELECT key, value from plato.Input2
    UNION ALL
    SELECT '' as key, value from plato.Input
)
ORDER BY key, value
;