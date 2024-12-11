/* syntax version 1 */
/* postgres can not */
SELECT
    count(*) AS s
FROM
    plato.Input
GROUP BY
    `dict`["a"]
ORDER BY
    s
;
