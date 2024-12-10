/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,
    count(DISTINCT value) AS cnt,
    ListSort(
        ListMap(
            aggregate_list(DISTINCT value), ($x) -> {
                RETURN DictItems($x)
            }
        )
    ) AS lst
FROM (
    SELECT
        key,
        AsDict(AsTuple(1, value)) AS value
    FROM
        Input
)
GROUP COMPACT BY
    key
ORDER BY
    key
;
