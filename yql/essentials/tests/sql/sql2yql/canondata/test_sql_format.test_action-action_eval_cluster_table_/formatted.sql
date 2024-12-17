/* syntax version 1 */
/* postgres can not */
$foo = CAST(Unicode::ToLower("PLATO"u) AS String);

INSERT INTO yt: $foo.Output
SELECT
    *
FROM
    yt: $foo.Input
WHERE
    key < '100'
ORDER BY
    key
;
