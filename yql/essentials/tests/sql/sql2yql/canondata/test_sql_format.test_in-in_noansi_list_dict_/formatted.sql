/* syntax version 1 */
/* postgres can not */
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN [2, 3, NULL], -- false
    2 IN [2, 3, NULL], -- true
    (1, 2) IN [(1, NULL), (2, 1)], -- false
    (1, 2) IN [(1, NULL), (1, 2)], -- true
    1 IN {2, 3, NULL}, -- false
    2 IN {2, 3, NULL}, -- true
    (1, 2) IN {(1, NULL), (2, 1)}, -- false
    (1, 2) IN {(1, NULL), (1, 2)}, -- true
;
