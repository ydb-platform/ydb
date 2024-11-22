--!syntax_pg
INSERT INTO plato."Output"
SELECT
    key, index+1 as index
FROM plato."Input"
ORDER BY index;