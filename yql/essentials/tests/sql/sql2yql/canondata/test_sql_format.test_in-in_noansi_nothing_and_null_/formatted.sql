/* syntax version 1 */
/* postgres can not */
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
PRAGMA config.flags('OptimizerFlags', 'SqlInWithNothingOrNull');

SELECT
    Nothing(Int32?) IN ListCreate(Int32), -- Nothing<Bool?>
    Nothing(Int32?) IN AsList(1), -- Nothing<Bool?>
;

SELECT
    Nothing(Int32?) IN AsDict(), -- Nothing<Bool?>
    Nothing(Int32?) IN AsDict((1, 2)) -- Nothing<Bool?>
;

SELECT
    Nothing(Int32?) IN AsTuple(), -- Nothing<Bool?>
    Nothing(Int32?) IN AsTuple(Just(1), 2) -- Nothing<Bool?>
;

SELECT
    NULL IN ListCreate(Int32), -- Nothing<Bool?>
    NULL IN AsList(1), -- Nothing<Bool?>
;

SELECT
    NULL IN AsDict(), -- Nothing<Bool?>
    NULL IN AsDict((1, 2)) -- Nothing<Bool?>
;

SELECT
    NULL IN AsTuple(), -- Nothing<Bool?>
    NULL IN AsTuple(Just(1), 2) -- Nothing<Bool?>
;
