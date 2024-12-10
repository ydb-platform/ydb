/* syntax version 1 */
/* postgres can not */
USE plato;

$input = (
    SELECT
        key,
        subkey,
        substring(value, 0, 1) == substring(value, 2, 1) AS value_from_a
    FROM
        Input
);

--insert into Output
SELECT
    key,
    subkey,
    count_if(value_from_a) AS approved,
    CAST(count_if(value_from_a) AS double) / count(*) AS approved_share,
    count(*) AS total
FROM
    $input
GROUP BY
    ROLLUP (key, subkey)
ORDER BY
    key,
    subkey
;
