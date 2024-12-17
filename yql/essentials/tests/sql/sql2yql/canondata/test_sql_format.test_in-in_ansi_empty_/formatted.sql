/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    NULL IN EmptyList(), -- false?
    NULL IN EmptyDict(), -- false?
    NULL IN (), -- false?
    1 IN EmptyList(), -- false
    1 IN EmptyDict(), -- false
    1 IN (), -- false
    NULL IN Nothing(EmptyList?), -- Nothing<Bool?>
    NULL IN Nothing(EmptyDict?), -- Nothing<Bool?>
    NULL IN Nothing(ParseType('Tuple<>?')), -- Nothing<Bool?>
;
