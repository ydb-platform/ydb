/* postgres can not */
/* syntax version 1 */
SELECT
    Enum('foo', Enum<'foo', 'bar'>) == AsEnum('foo'),
    Enum('foo', Enum<'foo', 'bar'>) == AsEnum('bar'),
    Enum('foo', Enum<'foo', 'bar'>) == AsEnum('baz')
;
