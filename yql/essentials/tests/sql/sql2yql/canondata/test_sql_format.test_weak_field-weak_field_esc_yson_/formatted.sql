/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    CAST(WeakField(subkey, 'string') AS uint32) AS subkey,
    WeakField(strE1, 'string'),
    YQL::FromYsonSimpleType(WeakField(strE1, "Yson"), AsAtom("String")) AS strE1overYson
FROM Input
ORDER BY
    subkey;
