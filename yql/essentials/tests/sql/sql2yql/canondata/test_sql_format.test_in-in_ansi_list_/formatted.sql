/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN AsList(2, 3, NULL), -- Nothing<Bool?>
    NULL IN ListCreate(Int32), -- false?
    NULL IN AsList(NULL), -- Nothing<Bool?>
    NULL IN AsList(1), -- Nothing<Bool?>
    (1, NULL) IN AsList((1, 1), (2, 2)), -- Nothing<Bool?>
    (1, NULL) IN AsList((2, 2), (3, 3)), -- false?
    (1, 2) IN AsList((1, NULL), (2, 2)), -- Nothing<Bool?>
    (1, 2) IN AsList((NULL, 1), (2, 1)), -- false?
    (1, 2) IN AsList((1, NULL), (2, 1)), -- Nothing<Bool?>
;

SELECT
    Just(1) IN AsList(1, 2, 3), -- true?
    1 IN AsList(Just(2), Just(3)), -- false?
;
