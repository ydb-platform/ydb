/* postgres can not */
SELECT DISTINCT
    key
FROM (
    SELECT
        AsTuple(Just(key), Just(key)) AS key,
        subkey,
        value
    FROM
        plato.Input
)
ORDER BY
    key
;
