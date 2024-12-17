/* syntax version 1 */
/* postgres can not */
SELECT
    key,
    subkey,
    value,
    2u * (2u * grouping(key) + grouping(subkey)) + grouping(value) AS ggg3,
FROM (
    SELECT
        subkey,
        value,
        CAST(key AS Int32) AS opt,
    FROM
        plato.Input
)
GROUP BY
    Unwrap(opt) AS key,
    CUBE (subkey, value)
ORDER BY
    key,
    subkey,
    value
;
