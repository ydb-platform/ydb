/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    Cast(WeakField(subkey, 'string') as uint32) as subkey,
    WeakField(strE1, 'string'),
    YQL::FromYsonSimpleType(WeakField(strE1, "Yson"), AsAtom("String")) AS strE1overYson
FROM Input
ORDER BY subkey;
