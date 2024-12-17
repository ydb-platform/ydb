/* syntax version 1 */
/* postgres can not */
SELECT
    sum(Math::Pow(CAST(subkey AS double), 2))
FROM
    plato.Input4
;
