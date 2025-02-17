/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN AsSet(2, 3, NULL), -- Nothing<Bool?>
    NULL IN SetCreate(Int32), -- false?
    NULL IN AsSet(NULL), -- Nothing<Bool?>
    NULL IN AsSet(1), -- Nothing<Bool?>
    (1, NULL) IN AsSet((1, 1), (2, 2)), -- Nothing<Bool?>
    (1, NULL) IN AsSet((2, 2), (3, 3)), -- false?
    (1, 2) IN AsSet((1, NULL), (2, 2)), -- Nothing<Bool?>
    (1, 2) IN AsSet((NULL, 1), (2, 1)), -- false?
    (1, 2) IN AsSet((1, NULL), (2, 1)), -- Nothing<Bool?>
;

SELECT
    Just(1) IN AsSet(1, 2, 3), -- true?
    1 IN AsSet(Just(2), Just(3)), -- false?
;
