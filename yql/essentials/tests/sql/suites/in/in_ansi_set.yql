/* syntax version 1 */
/* postgres can not */

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1         IN AsSet(2, 3, null),          -- Nothing<Bool?>
    null      IN SetCreate(Int32),           -- false?
    null      IN AsSet(null),                -- Nothing<Bool?>
    null      IN AsSet(1),                   -- Nothing<Bool?>
    (1, null) IN AsSet((1, 1), (2, 2)),      -- Nothing<Bool?>
    (1, null) IN AsSet((2, 2), (3, 3)),      -- false?
    (1, 2)    IN AsSet((1, null), (2, 2)),   -- Nothing<Bool?>
    (1, 2)    IN AsSet((null, 1), (2, 1)),   -- false?
    (1, 2)    IN AsSet((1, null), (2, 1)),   -- Nothing<Bool?>
;

SELECT
    Just(1)   IN AsSet(1, 2, 3),             -- true?
    1         IN AsSet(Just(2), Just(3)),    -- false?
;

