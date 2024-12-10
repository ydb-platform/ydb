/* postgres can not */
USE plato;

SELECT
    *
FROM
    Input4
WHERE
    subkey NOT IN (
        SELECT
            key || "0"
        FROM
            Input4
    )
ORDER BY
    key,
    subkey
;

SELECT
    *
FROM
    Input4
WHERE
    subkey IN COMPACT (
        SELECT
            key || "0"
        FROM
            Input4
    )
ORDER BY
    key,
    subkey
;
