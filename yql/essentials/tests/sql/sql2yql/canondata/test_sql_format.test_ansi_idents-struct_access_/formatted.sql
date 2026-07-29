--!ansi_lexer
$s = <|"a": 1, b: 2, `c`: 3|>;

SELECT
    $s."b"
;
