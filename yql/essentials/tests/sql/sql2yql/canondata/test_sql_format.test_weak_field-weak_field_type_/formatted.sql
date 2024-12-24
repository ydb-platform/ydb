/* postgres can not */
USE plato;

SELECT
    WeakField(key, DataType('String')),
    WeakField(subkey, OptionalType(DataType('String'))),
    WeakField(value, 'String')
FROM
    Input
;
