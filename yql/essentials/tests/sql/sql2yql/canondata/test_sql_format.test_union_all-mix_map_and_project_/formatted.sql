/* postgres can not */
/* kikimr can not */
PRAGMA yt.InferSchema;

SELECT
    *
FROM (
    SELECT
        key,
        '' AS value
    FROM plato.Input
    UNION ALL
    SELECT
        key,
        value
    FROM plato.Input2
    UNION ALL
    SELECT
        '' AS key,
        value
    FROM plato.Input
)
ORDER BY
    key,
    value;
