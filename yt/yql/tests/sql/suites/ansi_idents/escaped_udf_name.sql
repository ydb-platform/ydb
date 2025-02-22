--!ansi_lexer
/* syntax version 1 */
/* postgres can not */
use plato;

select `key` from Input where "String"::Contains("key", '7') order by key;
