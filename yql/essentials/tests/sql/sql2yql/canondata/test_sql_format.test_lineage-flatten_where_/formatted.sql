USE plato;

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        [key, key] AS key2,
        value
    FROM Input
    WHERE value > ''
)
    FLATTEN LIST BY key2;
