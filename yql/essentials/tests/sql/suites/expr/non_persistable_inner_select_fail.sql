PRAGMA Warning("error", '1104');

USE plato;

SELECT 1 FROM (
    SELECT
        YQL::NewMTRand(1) as x
    FROM Input
)
