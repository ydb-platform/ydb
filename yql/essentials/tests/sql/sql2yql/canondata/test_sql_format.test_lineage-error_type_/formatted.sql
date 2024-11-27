/* custom error:Uncompatible member foo types: Int32 and String*/
USE plato;

INSERT INTO Output
SELECT
    1 AS foo
FROM Input
UNION ALL
SELECT
    'x' AS foo
FROM Input;
