/* postgres can not */
-- right side depends on row
SELECT
    *
FROM
    plato.Input
WHERE
    AsTuple(key) IN (
        AsTuple("075"),
        AsTuple(key)
    )
ORDER BY
    key,
    subkey
;

-- left side is not a prefix of sort columns
SELECT
    *
FROM
    plato.Input
WHERE
    AsTuple(subkey, value) IN (
        AsTuple("1", "aaa"),
        AsTuple("3", "aaa")
    )
ORDER BY
    key,
    subkey
;

-- not a member on left side
SELECT
    *
FROM
    plato.Input
WHERE
    AsTuple(subkey, AsTuple(key, 1), value, key || "x") IN (
        AsTuple("1", AsTuple("075", 1), "abc", "075x"),
        AsTuple("3", AsTuple("023", 1), "aaa", "023x")
    )
ORDER BY
    key,
    subkey
;
