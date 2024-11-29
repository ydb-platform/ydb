/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE ACTION $action1($x) AS
    SELECT
        $x;
END DEFINE;

EVALUATE IF CAST(Unicode::ToUpper("i"u) AS String) == "I"
    DO $action1(1)
ELSE
    DO $action1(2);

EVALUATE IF CAST(Unicode::ToUpper("i"u) AS String) != "I"
    DO $action1(3);

EVALUATE IF CAST(Unicode::ToUpper("i"u) AS String) == "I"
    DO $action1(4);
