/* syntax version 1 */
/* postgres can not */

PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

SELECT
    1         IN [2, 3, null],          -- false
    2         IN [2, 3, null],          -- true
    (1, 2)    IN [(1, null), (2, 1)],   -- false
    (1, 2)    IN [(1, null), (1, 2)],   -- true

    1         IN {2, 3, null},          -- false
    2         IN {2, 3, null},          -- true
    (1, 2)    IN {(1, null), (2, 1)},   -- false
    (1, 2)    IN {(1, null), (1, 2)},   -- true
;

