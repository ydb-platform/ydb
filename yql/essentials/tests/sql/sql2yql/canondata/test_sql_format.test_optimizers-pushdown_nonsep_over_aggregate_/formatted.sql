USE plato;

PRAGMA config.flags("OptimizerFlags", "PushdownComplexFiltersOverAggregate");

SELECT
    *
FROM (
    SELECT
        key AS key,
        min(value) AS mv
    FROM
        Input
    GROUP BY
        key
)
WHERE
    AssumeNonStrict(200 > 100) AND (2000 > 1000) AND key != "911" AND (key < "150" AND mv != "ddd" OR key > "200")
;
