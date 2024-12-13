/* postgres can not */
USE plato;

$max =
    SELECT
        max(key)
    FROM
        Input
;

$list =
    SELECT
        key
    FROM
        Input
    WHERE
        subkey > '1'
;

SELECT
    *
FROM (
    SELECT
        if(key == $max, 'max', key) AS key,
        value
    FROM
        Input
)
WHERE
    key IN COMPACT $list
;
