/* syntax version 1 */
/* postgres can not */
DEFINE ACTION $a($x) AS
    $foo = CAST(Unicode::ToLower($x) AS String);

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
END DEFINE;

EVALUATE FOR $i IN AsList("PLATO"u) DO
    $a($i)
;
