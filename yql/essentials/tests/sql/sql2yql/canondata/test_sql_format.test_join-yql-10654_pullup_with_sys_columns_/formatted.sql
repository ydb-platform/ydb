/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableSimpleColumns;

$src =
    SELECT
        key,
        subkey || key AS subkey,
        value
    FROM
        Input
    UNION ALL
    SELECT
        *
    FROM
        AS_TABLE(ListCreate(Struct<key: String, subkey: String, value: String>))
;

SELECT
    a.key,
    a.subkey,
    b.value
FROM
    Input AS a
LEFT JOIN
    $src AS b
USING (key)
ORDER BY
    a.key
;
