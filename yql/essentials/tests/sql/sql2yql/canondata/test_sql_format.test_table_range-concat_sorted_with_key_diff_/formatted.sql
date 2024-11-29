/* postgres can not */
/* kikimr can not - range not supported */
SELECT
    *
FROM plato.concat("Input1", "Input2")
ORDER BY
    key,
    subkey;
