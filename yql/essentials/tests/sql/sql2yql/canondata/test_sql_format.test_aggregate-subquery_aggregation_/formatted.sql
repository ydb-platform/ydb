/* syntax version 1 */
/* postgres can not */
USE plato;

$total_count = (
    SELECT
        Count(*)
    FROM
        Input
);

SELECT
    common,
    count(*) AS rec_count,
    100. * count(*) / $total_count AS part_percent
FROM
    Input
GROUP BY
    subkey AS common
ORDER BY
    common
;
