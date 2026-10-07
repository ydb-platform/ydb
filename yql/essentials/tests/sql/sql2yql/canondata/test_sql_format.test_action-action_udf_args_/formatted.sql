DEFINE ACTION $f($x) AS
    SELECT
        $x
    ;
END DEFINE;

$x = CAST(Unicode::ToUpper("abc"u) AS String);

DO
    $f($x)
;
