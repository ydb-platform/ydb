/* postgres can not */
SELECT
    AsStruct(1 AS x) IN AsList(AsStruct(1 AS x), AsStruct(2 AS x))
;
