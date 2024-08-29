USE plato;

pragma config.flags("OptimizerFlags", "PushdownComplexFiltersOverAggregate");

SELECT * FROM (
    SELECT
        key as key,
        min(value) as mv
    FROM Input
    GROUP BY key
)
WHERE AssumeNonStrict(200 > 100) and (2000 > 1000) and key != "911" and (key < "150" and mv != "ddd" or key > "200");
