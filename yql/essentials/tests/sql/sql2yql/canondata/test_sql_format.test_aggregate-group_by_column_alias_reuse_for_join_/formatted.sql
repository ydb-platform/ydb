/* syntax version 1 */
/* postgres can not */
--INSERT INTO Output
SELECT
    a.key AS kk,

    --    kk,
    aggregate_list(b.subkey)
FROM
    plato.Input AS a
JOIN
    plato.Input4 AS b
ON
    a.subkey == b.key
GROUP BY
    a.key AS kk
ORDER BY
    kk
;
