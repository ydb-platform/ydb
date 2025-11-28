PRAGMA FeatureR010="prototype";

$input = SELECT * FROM AS_TABLE([
    <|time: 0,   value: 1u, name: "A"|>,
    <|time: 100, value: 2u, name: "A"|>,
    <|time: 200, value: 3u, name: "B"|>,
    <|time: 300, value: 3u, name: "B"|>,
    <|time: 400, value: 4u, name: "A"|>,
    <|time: 500, value: 5u, name: "A"|>,
]);

SELECT * FROM $input MATCH_RECOGNIZE (
    ORDER BY CAST(time AS Timestamp)
    MEASURES
        SUM(A.value + 1u + LENGTH(A.name)) + SUM(B.value + 1u + LENGTH(B.name)) AS aggr_expr,
        FIRST(A.value) AS first_a,
        LAST(A.value) AS last_a,
        COUNT(A.value) AS count_a,
        COUNT(DISTINCT A.value) AS count_distinct_a,
        AGGREGATE_LIST(A.value) AS aggrlist_a,
        AGGREGATE_LIST_DISTINCT(A.value) AS aggrlist_distinct_a,
        FIRST(B.value) AS first_b,
        LAST(B.value) AS last_b,
        COUNT(B.value) AS count_b,
        COUNT(DISTINCT B.value) AS count_distinct_b,
        AGGREGATE_LIST(B.value) AS aggrlist_b,
        AGGREGATE_LIST_DISTINCT(B.value) AS aggrlist_distinct_b,
        FIRST(C.value) AS first_c,
        LAST(C.value) AS last_c,
        COUNT(C.value) AS count_c,
        COUNT(DISTINCT C.value) AS count_distinct_c,
        AGGREGATE_LIST(C.value) AS aggrlist_c,
        AGGREGATE_LIST_DISTINCT(C.value) AS aggrlist_distinct_c
    PATTERN (A* B C* B A*)
    DEFINE
        A AS A.name = "A" AND COALESCE(FIRST(B.value + 1u + LENGTH(B.name)) = 5, TRUE),
        B AS B.name = "B" AND          FIRST(A.value + 1u + LENGTH(A.name)) = 3,
        C AS C.name = "C"
);
