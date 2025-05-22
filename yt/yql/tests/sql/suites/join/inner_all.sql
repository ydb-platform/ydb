PRAGMA DisableSimpleColumns;
use plato;
SELECT *
FROM Input2 AS a
JOIN Input3 AS b
ON a.value == b.value;
