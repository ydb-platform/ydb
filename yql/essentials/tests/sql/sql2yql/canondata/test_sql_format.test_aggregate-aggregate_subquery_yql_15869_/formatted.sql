USE plato;

$a = (
    SELECT
        CurrentUtcDate() AS _date,
        Just(1.0) AS parsed_lag
    FROM
        Input
);

SELECT
    SUM(parsed_lag)
FROM
    $a
;
