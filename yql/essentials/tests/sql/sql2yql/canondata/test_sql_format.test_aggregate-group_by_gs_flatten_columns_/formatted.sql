/* syntax version 1 */
/* postgres can not */
USE plato;

$input = (
    SELECT
        a.*,
        <|k1: 1, k2: 2|> AS s
    FROM
        Input AS a
);

SELECT
    key,
    subkey,
    some(k1) AS k1,
    some(k2) AS k2
FROM
    $input
    FLATTEN COLUMNS
GROUP BY
    GROUPING SETS (
        (key),
        (key, subkey)
    )
ORDER BY
    key,
    subkey
;
