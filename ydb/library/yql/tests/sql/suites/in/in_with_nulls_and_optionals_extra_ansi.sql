/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1         IN (2,3,null),                 -- Nothing<Bool?>
    1         IN (null),                     -- Nothing<Bool?>
    (1, null) IN ((1,2), (1, 3), (1, null)), -- Nothing<Bool?>
    (1, 1)    IN ((1,null)),                 -- Nothing<Bool?>
    (1, 1)    IN ((1,1),(2,null)),           -- Just(true)
    (1, null) IN ((1,2), (1,3)),             -- Nothing<Bool?>
    (2, null) IN ((1,2), (1,3)),             -- Just(false)
    (1, null) IN ();                         -- Just(false)
