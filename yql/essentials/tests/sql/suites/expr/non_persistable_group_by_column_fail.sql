PRAGMA Warning("error", '1104');

USE plato;

SELECT COUNT(*) FROM Input
GROUP BY YQL::NewMTRand(length(value)) as key;
