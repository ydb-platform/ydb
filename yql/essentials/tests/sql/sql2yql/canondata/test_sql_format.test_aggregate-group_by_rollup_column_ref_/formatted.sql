/* syntax version 1 */
/* postgres can not */
USE plato;

$input = (
    SELECT
        CAST(key AS int32) ?? 0 AS kk,
        CAST(subkey AS int32) ?? 0 AS sk,
        value
    FROM
        Input
);

--insert into Output
SELECT
    kk,
    sk,
    count(*) AS total_count
FROM
    $input
WHERE
    sk IN (23, 37, 75, 150,)
GROUP BY
    ROLLUP (kk, sk)
ORDER BY
    kk,
    sk,
    total_count
;
