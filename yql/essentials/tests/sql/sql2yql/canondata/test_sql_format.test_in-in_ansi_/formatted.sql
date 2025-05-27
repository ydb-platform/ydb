/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN (2, 3, NULL), -- Nothing<Bool?>
    NULL IN (), -- false?
    NULL IN (NULL), -- Nothing<Bool?>
    NULL IN (1), -- Nothing<Bool?>
    (1, NULL) IN ((1, 1), (2, 2)), -- Nothing<Bool?>
    (1, NULL) IN ((2, 2), (3, 3)), -- false?
    (1, 2) IN ((1, NULL), (2, 2)), -- Nothing<Bool?>
    (1, 2) IN ((NULL, 1), (2, 1)), -- false?
    (1, 2) IN ((1, NULL), (2, 1)), -- Nothing<Bool?>
    128 IN (128ut, 1t), -- true
;

SELECT
    Just(1) IN (1, 2, 3), -- true?
    1 IN (Just(2), Just(3)), -- false?
;
