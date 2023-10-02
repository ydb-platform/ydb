/* syntax version 1 */
/* postgres can not */

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1         IN AsList(2, 3, null),          -- Nothing<Bool?>
    null      IN ListCreate(Int32),           -- false?
    null      IN AsList(null),                -- Nothing<Bool?>
    null      IN AsList(1),                   -- Nothing<Bool?>
    (1, null) IN AsList((1, 1), (2, 2)),      -- Nothing<Bool?>
    (1, null) IN AsList((2, 2), (3, 3)),      -- false?
    (1, 2)    IN AsList((1, null), (2, 2)),   -- Nothing<Bool?>
    (1, 2)    IN AsList((null, 1), (2, 1)),   -- false?
    (1, 2)    IN AsList((1, null), (2, 1)),   -- Nothing<Bool?>
;

SELECT
    Just(1)   IN AsList(1, 2, 3),             -- true?
    1         IN AsList(Just(2), Just(3)),    -- false?
;

