/* syntax version 1 */
/* postgres can not */
SELECT
    ki,
    count(1)
FROM
    plato.Input
WHERE
    ki IN (75, 20)
GROUP BY
    CAST(key AS Uint32) AS ki
ORDER BY
    ki
;
