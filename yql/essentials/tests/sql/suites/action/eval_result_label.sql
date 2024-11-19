/* syntax version 1 */
/* postgres can not */
$x = CAST(Unicode::ToUpper("foo"u) AS String);
select 1 into result $x;
