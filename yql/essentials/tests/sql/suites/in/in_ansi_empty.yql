/* syntax version 1 */
/* postgres can not */

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    null      IN EmptyList(),                    -- false?
    null      IN EmptyDict(),                    -- false?
    null      IN (),                             -- false?
    1         IN EmptyList(),                    -- false
    1         IN EmptyDict(),                    -- false
    1         IN (),                             -- false
    null      IN Nothing(EmptyList?),            -- Nothing<Bool?>
    null      IN Nothing(EmptyDict?),            -- Nothing<Bool?>
    null      IN Nothing(ParseType("Tuple<>?")), -- Nothing<Bool?>
;

