PRAGMA DisableSimpleColumns;
use plato;
SELECT b.key
FROM Input2 as a
RIGHT JOIN Input3 as b ON a.key == b.key
WHERE a.key IS NULL
ORDER BY b.key;
