PRAGMA YqlSelect = 'force';

$rows = AsList(
    AsStruct(Just(-1) AS x),
    AsStruct(Just(2) AS x),
    AsStruct(Just(2) AS x),
    AsStruct(Nothing(Int32?) AS x)
);

$typed = (
    SELECT
        x AS i32,
        CAST(x AS Double?) AS f64
    FROM
        AS_TABLE($rows)
);

SELECT
    Spark::count(DISTINCT i32),
    Spark::min(DISTINCT i32),
    Spark::max(DISTINCT i32),
    Spark::avg(DISTINCT i32),
    Spark::sum(DISTINCT i32),
    Spark::count(DISTINCT f64),
    Spark::min(DISTINCT f64),
    Spark::max(DISTINCT f64),
    Spark::avg(DISTINCT f64),
    Spark::sum(DISTINCT f64)
FROM
    $typed
;

SELECT
    Spark::count(DISTINCT x),
    Spark::min(DISTINCT x),
    Spark::max(DISTINCT x),
    Spark::avg(DISTINCT x),
    Spark::sum(DISTINCT x)
FROM
    AS_TABLE($rows)
WHERE
    x IS NULL
;

SELECT
    Spark::count(DISTINCT x),
    Spark::min(DISTINCT x),
    Spark::max(DISTINCT x),
    Spark::avg(DISTINCT x),
    Spark::sum(DISTINCT x)
FROM
    AS_TABLE($rows)
WHERE
    FALSE
;

SELECT
    Spark::sum(DISTINCT x)
FROM
    AS_TABLE(AsList(
        AsStruct(Just(9223372036854775807L) AS x),
        AsStruct(Just(9223372036854775807L) AS x),
        AsStruct(Nothing(Int64?) AS x)
    ))
;
