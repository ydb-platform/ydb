/* yt can not */
PRAGMA CompactNamedExprs;

$a = (
    SELECT
        CAST(Unicode::ToUpper("o"u) AS String) || 'utpu'
);

$b = $a || CAST(Unicode::ToLower("T"u) AS String);

SELECT
    $b
;

SELECT
    $a || CAST(Unicode::ToLower("T"u) AS String)
;

SELECT
    *
FROM
    $a
;

SELECT
    'Outpu' IN $a
;

DEFINE SUBQUERY $sub() AS
    SELECT
        *
    FROM
        $a
    ;
END DEFINE;

SELECT
    *
FROM
    $sub()
;
