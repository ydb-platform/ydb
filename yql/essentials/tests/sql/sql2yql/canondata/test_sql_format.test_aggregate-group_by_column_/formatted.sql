/* syntax version 1 */
/* postgres can not */
-- order to have same on yt and yamr
SELECT
    count(1),
    z
FROM
    plato.Input
GROUP BY
    key AS z
ORDER BY
    z
;
