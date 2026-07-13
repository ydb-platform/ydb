--!ansi_lexer
PRAGMA warning("disable", "4510");

$foo = "YQL"::'(lambda ''(item) (Concat (String ''"foo\''") item))';

SELECT
    $foo('bar')
;
