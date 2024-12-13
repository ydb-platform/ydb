/* syntax version 1 */
/* postgres can not */
SELECT
    FormatType(Tuple<>),
    FormatType(Tuple< >),
    FormatType(Struct<>),
    FormatType(
        Struct<
            -- whitespace
             
        >
    )
;
