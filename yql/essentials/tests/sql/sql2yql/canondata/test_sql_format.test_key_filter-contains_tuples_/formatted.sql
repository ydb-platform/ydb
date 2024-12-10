/* postgres can not */
SELECT
    *
FROM
    plato.Input
WHERE
    AsTuple(key) IN (
        AsTuple("075"),
        AsTuple("037")
    )
ORDER BY
    key
;

SELECT
    *
FROM
    plato.Input
WHERE
    AsTuple(key, subkey) IN (
        AsTuple("075", "1"),
        AsTuple("023", "3")
    )
ORDER BY
    key
;

SELECT
    *
FROM
    plato.Input
WHERE
    AsTuple(key, subkey, 1 + 2, value) IN (
        AsTuple("075", "1", 3u, "abc"),
        AsTuple("023", "3", 1 + 1 + 1, "aaa")
    )
ORDER BY
    key
;

SELECT
    *
FROM
    plato.Input
WHERE
    AsTuple(subkey, AsTuple(key, 1), value, key) IN (
        AsTuple("1", AsTuple("075", 1), "abc", "075"),
        AsTuple("3", AsTuple("023", 1), "aaa", "023")
    )
ORDER BY
    key
;
