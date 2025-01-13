/* postgres can not */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    AsStruct(1 AS x) IN AsList(AsStruct(1 AS x), AsStruct(2 AS x))
;
