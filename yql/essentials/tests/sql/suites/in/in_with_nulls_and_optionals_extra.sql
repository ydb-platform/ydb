/* postgres can not */
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

SELECT
    1         IN (2,3,null),                 -- false
    1         IN (null),                     -- false
    (1, null) IN ((1,2), (1, 3), (1, null)), -- Nothing<Bool?>
    (1, 1)    IN ((1,null)),                 -- false
    (1, 1)    IN ((1,1),(2,null)),           -- true
    (1, null) IN ((1,2), (1,3)),             -- Nothing<Bool?>
    (2, null) IN ((1,2), (1,3)),             -- Nothing<Bool?>
    (1, null) IN ();                         -- Nothing<Bool?>
