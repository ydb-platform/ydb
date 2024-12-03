PRAGMA SimpleColumns;

--INSERT INTO plato.Output
SELECT
    100500 AS magic,
    t.*
FROM plato.Input
    AS t;
