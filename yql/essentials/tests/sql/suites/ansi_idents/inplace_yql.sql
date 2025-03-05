--!ansi_lexer
/* syntax version 1 */
/* postgres can not */
pragma warning("disable", "4510");

$foo = "YQL"::'(lambda ''(item) (Concat (String ''"foo\''") item))';

select $foo('bar');
