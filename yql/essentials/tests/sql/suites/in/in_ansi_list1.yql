/* syntax version 1 */
/* postgres can not */

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1         IN [2, 3, null],          -- Nothing<Bool?>
    2         IN [2, 3, null],          -- true?
    (1, 2)    IN [(1, null), (2, 1)],   -- Nothing<Bool?>
    (1, 2)    IN [(1, null), (1, 2)],   -- true?
;

