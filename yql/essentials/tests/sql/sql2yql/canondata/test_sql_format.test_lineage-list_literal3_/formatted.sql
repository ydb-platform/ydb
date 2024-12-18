USE plato;

$key = (
    SELECT
        min(key)
    FROM
        Input
);

$value = (
    SELECT
        min(value)
    FROM
        Input
);

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        $key AS key
    UNION ALL
    SELECT
        $value AS val
);
