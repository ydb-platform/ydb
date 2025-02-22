/* syntax version 1 */
/* postgres can not */
use plato;

define subquery $q($name) as
    define subquery $nested() as
        select $name;
    end define;

    process $nested();
end define;

process $q(CAST(Unicode::ToUpper("foo"u) AS String));
