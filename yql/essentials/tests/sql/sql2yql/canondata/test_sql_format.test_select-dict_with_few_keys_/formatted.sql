/* syntax version 1 */
/* postgres can not */
USE plato;

$dict = (
    SELECT
        AsDict(
            AsTuple("key", CAST(key AS uint32) ?? 0),
            AsTuple("sk", CAST(subkey AS uint32) ?? 1),
            AsTuple("str", CAST(ByteAt(value, 0) AS uint32) ?? 256)
        ) AS dd
    FROM Input
);

--INSERT INTO Output
SELECT
    dd['key'] AS key,
    dd['str'] AS zz
FROM $dict
    AS d
ORDER BY
    key,
    zz;
