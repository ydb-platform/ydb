/* postgres can not */
/* syntax version 1 */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    ENUM ('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('foo')
    ),
    ENUM ('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('bar')
    ),
    ENUM ('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('foo'),
        AsEnum('bar')
    ),
    ENUM ('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('bar'),
        AsEnum('baz')
    ),
    ENUM ('foo', Enum<'foo', 'bar'>) IN [
        AsEnum('foo'),
        AsEnum('bar')
    ],
    ENUM ('foo', Enum<'foo', 'bar'>) IN {
        AsEnum('bar'),
        AsEnum('baz')
    }
;
