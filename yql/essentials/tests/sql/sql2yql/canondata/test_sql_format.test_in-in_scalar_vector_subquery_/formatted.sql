/* postgres can not */
USE plato;

$bar = (
    SELECT
        "1"
    UNION ALL
    SELECT
        "2"
);

$barr = (
    SELECT
        "1" AS subkey
    UNION ALL
    SELECT
        "2" AS subkey
);

SELECT
    "1" IN $bar,
    "2" IN $bar;

SELECT
    "3" IN $bar;

SELECT
    "1" IN AsList($barr),
    "2" IN AsList($barr);

SELECT
    "3" IN AsList($barr);

SELECT
    *
FROM Input
WHERE subkey IN $bar
ORDER BY
    subkey;

SELECT
    *
FROM Input
WHERE subkey IN AsList($barr)
ORDER BY
    subkey;

-- same content as $bar
$baz = (
    SELECT
        subkey
    FROM Input
    WHERE subkey == "1" OR subkey == "2"
);

$bazz = (
    SELECT
        subkey
    FROM Input
    WHERE subkey < "3"
    ORDER BY
        subkey ASC
    LIMIT 1
);

SELECT
    "1" IN $baz,
    "2" IN $baz;

SELECT
    "3" IN $baz;

SELECT
    "1" IN AsList($bazz),
    "2" IN AsList($bazz);

SELECT
    "3" IN AsList($bazz);

SELECT
    *
FROM Input
WHERE subkey IN $baz
ORDER BY
    subkey;

SELECT
    *
FROM Input
WHERE subkey IN AsList($bazz)
ORDER BY
    subkey;
