/* syntax version 1 */
/* postgres can not */
USE plato;

define action $action1($x) as
  select $x;
end define;

evaluate if CAST(Unicode::ToUpper("i"u) AS String) == "I"
    do $action1(1)
else
    do $action1(2);

evaluate if CAST(Unicode::ToUpper("i"u) AS String) != "I"
    do $action1(3);

evaluate if CAST(Unicode::ToUpper("i"u) AS String) == "I"
    do $action1(4);
