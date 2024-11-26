/* syntax version 1 */
/* postgres can not */
PRAGMA sampleselect;

SELECT
    kk,
    ListSort(aggregate_list(t2.key))
FROM plato.Input
    AS t1
INNER JOIN plato.Input
    AS t2
ON t1.key == t2.subkey
GROUP BY
    t1.key AS kk
ORDER BY
    kk;
