PRAGMA Warning("error", '1104');

USE plato;

SELECT key, value FROM Input
ORDER BY YQL::NewMTRand(length(value));
