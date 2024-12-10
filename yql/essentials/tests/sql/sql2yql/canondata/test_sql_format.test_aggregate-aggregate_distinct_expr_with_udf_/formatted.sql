/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    Math::Round(count(DISTINCT Math::Round(CAST(key AS Int32))) / 100.0, -2)
FROM
    Input2
;
