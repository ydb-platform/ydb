/* syntax version 1 */
/* postgres can not */
$x = CAST(Unicode::ToUpper("foo"u) AS String);

SELECT
    1
INTO RESULT $x;
