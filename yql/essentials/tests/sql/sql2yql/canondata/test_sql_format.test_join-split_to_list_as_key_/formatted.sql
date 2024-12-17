/* syntax version 1 */
PRAGMA DisableSimpleColumns;

USE plato;

FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    a.key == String::SplitToList(b.key, '_')[0]
SELECT
    *
ORDER BY
    a.key,
    a.subkey,
    b.subkey
;
