/* postgres can not */
USE plato;

$rc = (
    SELECT
        count(*)
    FROM
        Input
);

$sample_size = 10;

SELECT
    *
FROM
    Input
    TABLESAMPLE BERNOULLI (MIN_OF($sample_size * 100.0 / $rc, 100.0))
ORDER BY
    key
;
