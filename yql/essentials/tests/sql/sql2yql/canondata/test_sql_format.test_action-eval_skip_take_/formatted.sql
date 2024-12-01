/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    *
FROM Input
ORDER BY
    key
LIMIT length(CAST(Unicode::ToUpper("a"u) AS String)) OFFSET length(CAST(Unicode::ToUpper("bc"u) AS String));
