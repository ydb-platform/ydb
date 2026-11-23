PRAGMA YqlSelect = 'force';

$rows = AsList(
    AsStruct(Just(-1) AS x),
    AsStruct(Just(2) AS x),
    AsStruct(Just(2) AS x),
    AsStruct(Nothing(Int32?) AS x)
);

$typed = (
    SELECT
        CAST(x AS Int8?) AS i8,
        CAST(x AS Int16?) AS i16,
        x AS i32,
        CAST(x AS Int64?) AS i64,
        CAST(x AS Float?) AS f32,
        CAST(x AS Double?) AS f64
    FROM
        AS_TABLE($rows)
);

SELECT
    Spark::count(*) AS rows,
    Spark::count(i8) AS count_i8,
    Spark::min(i8) AS min_i8,
    Spark::max(i8) AS max_i8,
    Spark::avg(i8) AS avg_i8,
    Spark::sum(i8) AS sum_i8,
    Spark::count(i16) AS count_i16,
    Spark::min(i16) AS min_i16,
    Spark::max(i16) AS max_i16,
    Spark::avg(i16) AS avg_i16,
    Spark::sum(i16) AS sum_i16,
    Spark::count(i32) AS count_i32,
    Spark::min(i32) AS min_i32,
    Spark::max(i32) AS max_i32,
    Spark::avg(i32) AS avg_i32,
    Spark::sum(i32) AS sum_i32,
    Spark::count(i64) AS count_i64,
    Spark::min(i64) AS min_i64,
    Spark::max(i64) AS max_i64,
    Spark::avg(i64) AS avg_i64,
    Spark::sum(i64) AS sum_i64,
    Spark::count(f32) AS count_f32,
    Spark::min(f32) AS min_f32,
    Spark::max(f32) AS max_f32,
    Spark::avg(f32) AS avg_f32,
    Spark::sum(f32) AS sum_f32,
    Spark::count(f64) AS count_f64,
    Spark::min(f64) AS min_f64,
    Spark::max(f64) AS max_f64,
    Spark::avg(f64) AS avg_f64,
    Spark::sum(f64) AS sum_f64
FROM
    $typed
;

SELECT
    Spark::count(x),
    Spark::count(*),
    Spark::min(x),
    Spark::max(x),
    Spark::avg(x),
    Spark::sum(x)
FROM
    AS_TABLE($rows)
WHERE
    x IS NULL
;

SELECT
    Spark::count(x),
    Spark::count(*),
    Spark::min(x),
    Spark::max(x),
    Spark::avg(x),
    Spark::sum(x)
FROM
    AS_TABLE($rows)
WHERE
    FALSE
;

SELECT
    Spark::count(1),
    Spark::min(2),
    Spark::max(3),
    Spark::avg(4),
    Spark::sum(5)
;

SELECT
    Spark::sum(x) + Spark::count(x) AS result
FROM
    AS_TABLE($rows)
HAVING
    Spark::sum(x) > 0
;
