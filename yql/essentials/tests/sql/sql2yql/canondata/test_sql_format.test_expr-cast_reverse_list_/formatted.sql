/* postgres can not */
/* syntax version 1 */
SELECT
    CAST(ListReverse(ListExtend(['1', '2', '3'], ['4', '5', '6'])) AS List<Int64>)
;
