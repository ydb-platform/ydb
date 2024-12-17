USE plato;

$data = (
    SELECT
        key,
        subkey,
        value
    FROM
        Input
    GROUP BY
        GROUPING SETS (
            (key, subkey),
            (subkey, value)
        )
);

INSERT INTO @tmp WITH TRUNCATE
SELECT
    b.value
FROM
    $data AS a
LEFT JOIN
    Input AS b
USING (key);
