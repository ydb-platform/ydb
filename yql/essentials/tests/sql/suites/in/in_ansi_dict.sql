/* syntax version 1 */
/* postgres can not */

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1         IN AsDict((2, "x"), (3, "y"), (null, "z")),       -- Nothing<Bool?>
    null      IN DictCreate(Int32, String?),                    -- false?
    null      IN AsDict((null, "foo")),                         -- Nothing<Bool?>
    null      IN AsDict((1, "bar")),                            -- Nothing<Bool?>
    (1, null) IN AsDict(((1, 1), null), ((2, 2), "foo")),       -- Nothing<Bool?>
    (1, null) IN AsDict(((2, 2), "foo"), ((3, 3), "bar")),      -- false?
    (1, 2)    IN AsDict(((1, null), "foo"), ((2, 2), "bar")),   -- Nothing<Bool?>
    (1, 2)    IN AsDict(((null, 1), "foo"),  ((2, 1), null)),   -- false?
    (1, 2)    IN AsDict(((1, null), "foo"), ((2, 1), "bar")),   -- Nothing<Bool?>
;

SELECT
    Just(1)   IN AsDict((1, "foo"), (2, "bar"), (3, null)),     -- true?
    1         IN AsDict((Just(2), null), (Just(3), "bar")),     -- false?
;
