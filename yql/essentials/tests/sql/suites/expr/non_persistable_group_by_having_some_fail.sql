PRAGMA Warning("error", '1104');

USE plato;

SELECT
    key
FROM Input
GROUP BY key
HAVING Yql::NextMtRand(SOME(YQL::NewMTRand(1))).0 > 100;
