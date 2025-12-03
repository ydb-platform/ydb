/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;
PRAGMA config.flags('OptimizerFlags', 'SqlInWithNothingOrNull');

SELECT
    Nothing(Int32?) IN ListCreate(Int32), -- false?
    Nothing(Int32?) IN AsList(1), -- Nothing<Bool?>
;

SELECT
    Nothing(Int32?) IN AsDict(), -- false?
    Nothing(Int32?) IN AsDict((1, 2)) -- Nothing<Bool?>
;

SELECT
    Nothing(Int32?) IN AsTuple(), -- false?
    Nothing(Int32?) IN AsTuple(Just(1), 2) -- Nothing<Bool?>
;

SELECT
    NULL IN ListCreate(Int32), -- false?
    Nothing(Int32?) IN AsList(1), -- Nothing<Bool?>
;

SELECT
    NULL IN AsDict(), -- false?
    Nothing(Int32?) IN AsDict((1, 2)) -- Nothing<Bool?>
;

SELECT
    NULL IN AsTuple(), -- false?
    Nothing(Int32?) IN AsTuple(Just(1), 2) -- Nothing<Bool?>
;
