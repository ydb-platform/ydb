/* syntax version 1 */
/* postgres can not */
$foo = CAST(Unicode::ToLower("PLATO"u) AS String);

USE yt: $foo;

INSERT INTO Output
SELECT
    *
FROM
    Input
WHERE
    key < "100"
ORDER BY
    key
;
