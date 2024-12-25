/* postgres can not */
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN (2, 3, NULL), -- false
    1 IN (NULL), -- false
    (1, NULL) IN ((1, 2), (1, 3), (1, NULL)), -- Nothing<Bool?>
    (1, 1) IN ((1, NULL)), -- false
    (1, 1) IN ((1, 1), (2, NULL)), -- true
    (1, NULL) IN ((1, 2), (1, 3)), -- Nothing<Bool?>
    (2, NULL) IN ((1, 2), (1, 3)), -- Nothing<Bool?>
    (1, NULL) IN ()
; -- Nothing<Bool?>
