/* syntax version 1 */
/* postgres can not */
SELECT
    aggregate_list(a.k),
    aval
FROM (
    SELECT
        CAST(subkey AS uint32) AS k,
        value AS val
    FROM plato.Input
)
    AS a
LEFT ONLY JOIN (
    SELECT
        CAST(key AS uint32) AS k,
        CAST(subkey AS uint32) AS s
    FROM plato.Input
)
    AS b
USING (k)
GROUP BY
    a.val AS aval;
