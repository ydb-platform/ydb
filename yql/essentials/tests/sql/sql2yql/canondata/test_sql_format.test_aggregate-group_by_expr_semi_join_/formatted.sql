/* syntax version 1 */
/* postgres can not */
SELECT
    ListSort(aggregate_list(b.uk)),
    ListSort(aggregate_list(b.uk)),
    bus
FROM (
    SELECT
        CAST(key AS uint32) AS uk
    FROM plato.Input
)
    AS a
RIGHT SEMI JOIN (
    SELECT
        CAST(key AS uint32) AS uk,
        CAST(subkey AS uint32) AS us
    FROM plato.Input
)
    AS b
USING (uk)
GROUP BY
    b.us AS bus
ORDER BY
    bus;
