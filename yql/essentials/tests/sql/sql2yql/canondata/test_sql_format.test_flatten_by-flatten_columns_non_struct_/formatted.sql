/* postgres can not */
SELECT
    *
FROM (
    SELECT
        1,
        AsStruct(2 AS foo),
        Just(AsStruct(3 AS bar)),
        Just(AsStruct(Just(4) AS qwe))
)
    FLATTEN COLUMNS
;
