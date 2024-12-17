/* syntax version 1 */
USE plato;

$dict = AsDict(("800", "foo"));

SELECT 
    lookup_result
FROM Input
GROUP BY $dict[key] ?? "bar" AS lookup_result
ORDER BY lookup_result;
