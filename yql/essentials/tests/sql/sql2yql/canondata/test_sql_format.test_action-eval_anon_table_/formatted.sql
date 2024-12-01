/* syntax version 1 */
/* postgres can not */
USE plato;
$a = CAST(Unicode::ToUpper("T"u) AS String) || "able";
$b = CAST(Unicode::ToUpper("T"u) AS String) || "able";

INSERT INTO @$a
SELECT
    1 AS x;
COMMIT;

SELECT
    *
FROM @$b;
