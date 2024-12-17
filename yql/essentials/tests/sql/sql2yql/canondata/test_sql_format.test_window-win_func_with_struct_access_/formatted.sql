/* postgres can not */
USE plato;

$input = (
    SELECT
        CAST(key AS int32) / 100 AS key_hundred,
        AsStruct(
            CAST(key AS int32) AS key,
            CAST(subkey AS int32) AS subkey
        ) AS `struct`,
        value
    FROM
        Input AS inSrc
);

--INSERT INTO Output
SELECT
    key_hundred AS a_part,
    `struct`.key - lead(`struct`.key, 1) OVER w AS keyDiff,
    value
FROM
    $input AS outSrc
WINDOW
    w AS (
        PARTITION BY
            key_hundred
        ORDER BY
            `struct`.key,
            value
    )
ORDER BY
    a_part,
    value
;
