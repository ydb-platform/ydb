/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN (2, 3, NULL), -- Nothing<Bool?>
    1 IN (NULL), -- Nothing<Bool?>
    (1, NULL) IN ((1, 2), (1, 3), (1, NULL)), -- Nothing<Bool?>
    (1, 1) IN ((1, NULL)), -- Nothing<Bool?>
    (1, 1) IN ((1, 1), (2, NULL)), -- Just(true)
    (1, NULL) IN ((1, 2), (1, 3)), -- Nothing<Bool?>
    (2, NULL) IN ((1, 2), (1, 3)), -- Just(false)
    (1, NULL) IN ()
; -- Just(false)
