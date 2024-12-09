/* syntax version 1 */
/* postgres can not */

PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

SELECT
    null      IN EmptyList(),                    -- Nothing<Bool?>
    null      IN EmptyDict(),                    -- Nothing<Bool?>
    null      IN (),                             -- Nothing<Bool?>
    1         IN EmptyList(),                    -- false
    1         IN EmptyDict(),                    -- false
    1         IN (),                             -- false
    null      IN Nothing(EmptyList?),            -- Nothing<Bool?>
    null      IN Nothing(EmptyDict?),            -- Nothing<Bool?>
    null      IN Nothing(ParseType("Tuple<>?")), -- Nothing<Bool?>
;

