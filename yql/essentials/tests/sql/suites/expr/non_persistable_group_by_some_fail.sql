PRAGMA Warning("error", '1104');

USE plato;

SELECT SOME(YQL::NewMTRand(1)) FROM Input
GROUP BY key;
