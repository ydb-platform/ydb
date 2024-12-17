/* syntax version 1 */
/* postgres can not */
USE plato;

$input = (
    SELECT
        a.*,
        [1, 2] AS lst
    FROM
        Input AS a
);

SELECT
    key,
    subkey,
    some(lst) AS lst_count
FROM
    $input
    FLATTEN LIST BY lst
WHERE
    lst != 1
GROUP BY
    GROUPING SETS (
        (key),
        (key, subkey)
    )
ORDER BY
    key,
    subkey
;
