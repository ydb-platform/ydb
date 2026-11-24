PRAGMA YqlSelect = 'force';

$rows = AsList(
    AsStruct(1 AS id, 1 AS key, Just(1) AS value),
    AsStruct(2 AS id, 1 AS key, Just(1) AS value),
    AsStruct(3 AS id, 1 AS key, Just(3) AS value),
    AsStruct(4 AS id, 2 AS key, Just(5) AS value),
    AsStruct(5 AS id, 2 AS key, Nothing(Int32?) AS value)
);

SELECT
    ListSort(EnsureType(Aggregate_List(value), List<Int32>)) AS values,
    ListSort(Agg_List(value)) AS alias_values,
    ListSort(Aggr_List(value)) AS other_alias_values,
    ListSort(Aggregate_List(DISTINCT value)) AS distinct_values,
    ListSort(EnsureType(Aggregate_List_Distinct(value), List<Int32>)) AS set_values,
    ListSort(Agg_List_Distinct(value)) AS alias_set_values,
    ListSort(Aggr_List_Distinct(value)) AS other_alias_set_values,
    ListSort(Aggregate_List(value, 0)) AS unlimited_values,
    ListLength(Aggregate_List(value, 2)) AS limited_size,
    ListLength(Aggregate_List_Distinct(value, 2)) AS limited_set_size,
    ListLength(ListUniq(Aggregate_List_Distinct(value, 2))) AS limited_set_unique_size,
    ListAll(
        ListMap(
            Aggregate_List_Distinct(value, 2), ($item) -> {
                RETURN $item IN (1, 3, 5);
            }
        )
    ) AS limited_set_members_valid
FROM
    AS_TABLE($rows)
;

SELECT
    key,
    ListSort(AggList(value + 1)) AS values
FROM
    AS_TABLE($rows)
GROUP BY
    key
HAVING
    ListLength(AggList(value)) > 0
ORDER BY
    key
;

SELECT
    ListSort(EnsureType(Aggregate_List(value), List<Int32>)) AS values,
    ListSort(Aggregate_List_Distinct(value)) AS set_values
FROM
    AS_TABLE($rows)
WHERE
    FALSE
;

SELECT
    ListSort(Aggregate_List(value)) AS values,
    ListSort(Aggregate_List_Distinct(value)) AS set_values
FROM
    AS_TABLE($rows)
WHERE
    value IS NULL
;

SELECT
    Aggregate_List(NULL) AS values,
    Aggregate_List_Distinct(NULL) AS set_values
FROM
    AS_TABLE($rows)
;

SELECT
    ListSort(
        Aggregate_List(AsStruct(value AS item)), ($row) -> {
            RETURN $row.item;
        }
    ) AS struct_values,
    ListSort(Aggregate_List(AsList(value))) AS list_values
FROM
    AS_TABLE($rows)
;

SELECT
    id,
    key,
    value,
    ListSort(Aggregate_List(value) OVER w) AS values,
    ListSort(Aggregate_List_Distinct(value) OVER w) AS set_values,
    ListLength(Aggregate_List(value, 1) OVER w) AS limited_size,
    ListSort(Aggregate_List(value) OVER next_row) AS next_values,
    ListSort(Aggregate_List_Distinct(value) OVER next_row) AS next_set_values
FROM
    AS_TABLE($rows)
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    next_row AS (
        PARTITION BY
            key
        ORDER BY
            id
        ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
    )
ORDER BY
    id
;
