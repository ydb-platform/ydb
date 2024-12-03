/* custom error:Expected hashable and equatable type for key column: key, but got: Resource<'MTRand'>*/
PRAGMA Warning("error", '1104');

USE plato;

SELECT COUNT(*) FROM Input
GROUP BY YQL::NewMTRand(length(value)) as key;
