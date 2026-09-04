PRAGMA config.flags('OptimizerFlags', 'PushdownFiltersOverWindow');
USE plato;

$C = (
SELECT
    key,
    value,
    ROW_NUMBER() OVER (PARTITION BY key ORDER BY value) AS grp
FROM Input
);

INSERT INTO Output
SELECT 
    c.*,
FROM $C AS c
WHERE
    value = "abc"
ORDER BY 
    value

