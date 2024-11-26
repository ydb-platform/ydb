/* postgres can not */
USE plato;

SELECT
    dt_yson,
    lst_yson,
    dt,
    lst
FROM Input
    WITH COLUMNS Struct<lst_yson: List<int32>?, dt_yson: Date?>;
