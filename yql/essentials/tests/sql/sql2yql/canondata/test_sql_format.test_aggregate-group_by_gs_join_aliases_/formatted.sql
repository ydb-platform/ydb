/* syntax version 1 */
/* postgres can not */
SELECT
    k1,
    k2,
    b.subkey AS kk2,
    SOME(a.value) AS val
FROM
    plato.Input AS a
JOIN
    plato.Input AS b
USING (key)
GROUP BY
    GROUPING SETS (
        (a.key AS k1, b.subkey AS k2),
        (k1),
        (b.subkey)
    )
ORDER BY
    k1,
    kk2
;
