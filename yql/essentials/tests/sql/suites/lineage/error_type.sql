USE plato;

INSERT INTO Output
SELECT
    1 as foo
FROM Input

UNION ALL

SELECT
    'x' as foo
FROM Input

