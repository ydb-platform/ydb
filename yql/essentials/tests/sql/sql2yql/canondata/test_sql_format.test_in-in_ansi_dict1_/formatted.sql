/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN {2, 3, NULL}, -- Nothing<Bool?>
    2 IN {2, 3, NULL}, -- true?
    (1, 2) IN {(1, NULL), (2, 1)}, -- Nothing<Bool?>
    (1, 2) IN {(1, NULL), (1, 2)}, -- true?
;
