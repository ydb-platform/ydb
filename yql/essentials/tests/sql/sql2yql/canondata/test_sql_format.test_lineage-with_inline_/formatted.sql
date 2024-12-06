USE plato;

INSERT INTO Output
SELECT
    key AS key,
    "" AS subkey,
    "value:" || value AS value
FROM Input
    WITH INLINE;
