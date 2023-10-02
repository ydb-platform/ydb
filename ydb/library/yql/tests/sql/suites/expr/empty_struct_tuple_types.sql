/* syntax version 1 */
/* postgres can not */
select
    FormatType(Tuple<>),
    FormatType(Tuple< >),
    FormatType(Struct<>),
    FormatType(Struct<
              -- whitespace
              >);
