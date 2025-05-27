/* postgres can not */
/* syntax version 1 */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    Enum('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('foo')
    ),
    Enum('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('bar')
    ),
    Enum('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('foo'),
        AsEnum('bar')
    ),
    Enum('foo', Enum<'foo', 'bar'>) IN (
        AsEnum('bar'),
        AsEnum('baz')
    ),
    Enum('foo', Enum<'foo', 'bar'>) IN [
        AsEnum('foo'),
        AsEnum('bar')
    ],
    Enum('foo', Enum<'foo', 'bar'>) IN {
        AsEnum('bar'),
        AsEnum('baz')
    }
;
