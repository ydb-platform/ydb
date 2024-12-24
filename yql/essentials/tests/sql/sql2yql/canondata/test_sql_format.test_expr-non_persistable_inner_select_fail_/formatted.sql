/* custom error:Persistable required*/
PRAGMA Warning('error', '1104');

USE plato;

SELECT
    1
FROM (
    SELECT
        YQL::NewMTRand(1) AS x
    FROM
        Input
);
