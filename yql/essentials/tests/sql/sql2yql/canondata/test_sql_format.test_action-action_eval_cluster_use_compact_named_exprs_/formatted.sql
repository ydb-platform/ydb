/* yt can not */
PRAGMA CompactNamedExprs;

$foo = CAST(Unicode::ToLower("PLATO"u) AS String);

INSERT INTO yt: $foo.Output
SELECT
    *
FROM
    yt: $foo.Input
WHERE
    key < "100"
ORDER BY
    key
;

DEFINE ACTION $bar() AS
    $x = CAST(Unicode::ToLower("PLaTO"u) AS String);

    INSERT INTO yt: $x.Output
    SELECT
        *
    FROM
        yt: $foo.Input
    WHERE
        key < "100"
    ORDER BY
        key
    ;
END DEFINE;

DO
    $bar()
;
