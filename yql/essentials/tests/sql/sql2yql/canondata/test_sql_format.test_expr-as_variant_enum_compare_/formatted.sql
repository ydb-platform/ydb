/* postgres can not */
/* syntax version 1 */
SELECT
    ENUM ('foo', Enum<'foo', 'bar'>) == AsEnum('foo'),
    ENUM ('foo', Enum<'foo', 'bar'>) == AsEnum('bar'),
    ENUM ('foo', Enum<'foo', 'bar'>) == AsEnum('baz')
;
