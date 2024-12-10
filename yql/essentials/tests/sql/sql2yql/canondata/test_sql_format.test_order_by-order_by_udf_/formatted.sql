/* postgres can not */
SELECT
    *
FROM
    plato.Input
ORDER BY
    Math::Pow(CAST(subkey AS double), 2)
;
