PRAGMA EmitAggApply;

$x = (
    SELECT
        *
    FROM
        as_table([<|x: 1, y: 2, z: 3|>, <|x: 4, y: 5, z: 6|>])
);

SELECT
    count(x + y)
FROM
    $x
;
