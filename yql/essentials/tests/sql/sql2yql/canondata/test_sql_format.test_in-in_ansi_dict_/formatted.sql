/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN AsDict((2, 'x'), (3, 'y'), (NULL, 'z')), -- Nothing<Bool?>
    NULL IN DictCreate(Int32, String?), -- false?
    NULL IN AsDict((NULL, 'foo')), -- Nothing<Bool?>
    NULL IN AsDict((1, 'bar')), -- Nothing<Bool?>
    (1, NULL) IN AsDict(((1, 1), NULL), ((2, 2), 'foo')), -- Nothing<Bool?>
    (1, NULL) IN AsDict(((2, 2), 'foo'), ((3, 3), 'bar')), -- false?
    (1, 2) IN AsDict(((1, NULL), 'foo'), ((2, 2), 'bar')), -- Nothing<Bool?>
    (1, 2) IN AsDict(((NULL, 1), 'foo'), ((2, 1), NULL)), -- false?
    (1, 2) IN AsDict(((1, NULL), 'foo'), ((2, 1), 'bar')), -- Nothing<Bool?>
;

SELECT
    Just(1) IN AsDict((1, 'foo'), (2, 'bar'), (3, NULL)), -- true?
    1 IN AsDict((Just(2), NULL), (Just(3), 'bar')), -- false?
;
