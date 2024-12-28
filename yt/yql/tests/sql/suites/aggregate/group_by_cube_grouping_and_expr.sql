/* syntax version 1 */
/* postgres can not */
SELECT
    key,
    subkey,
    value,
    2u * (2u * grouping(key) + grouping(subkey)) + grouping(value) as ggg3,
FROM
    (
    SELECT
        subkey,
        value,
        cast(key as Int32) as opt,
    FROM
    plato.Input
    )
GROUP BY
    Unwrap(opt) AS key,
    CUBE(subkey, value)
ORDER BY key, subkey, value;
