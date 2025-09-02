/* syntax version 1 */
/* postgres can not */
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

SELECT
    NULL IN EmptyList(), -- Nothing<Bool?>
    NULL IN EmptyDict(), -- Nothing<Bool?>
    NULL IN (), -- Nothing<Bool?>
    1 IN EmptyList(), -- false
    1 IN EmptyDict(), -- false
    1 IN (), -- false
    NULL IN Nothing(EmptyList?), -- Nothing<Bool?>
    NULL IN Nothing(EmptyDict?), -- Nothing<Bool?>
    NULL IN Nothing(ParseType('Tuple<>?')), -- Nothing<Bool?>
;
