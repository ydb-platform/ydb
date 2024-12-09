/* syntax version 1 */
/* postgres can not */
define action $f($x) as
    select $x;
end define;

$x = CAST(Unicode::ToUpper("abc"u) AS String);
do $f($x);
