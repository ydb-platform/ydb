/* postgres can not */
SELECT
    *
FROM plato.Input
ORDER BY
    Math::Pow(CAST(subkey AS double), 2),
    Math::Pow(CAST(key AS double), 2);
