/* postgres can not */

SELECT *
FROM plato.Input
ORDER BY Math::Pow(cast(subkey as double), 2), Math::Pow(cast(key as double), 2);
