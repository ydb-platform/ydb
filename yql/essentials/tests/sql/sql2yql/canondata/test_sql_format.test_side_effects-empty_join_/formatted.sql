/* custom error: Condition violated */
$input1 = ListTake([<|x: 1|>], 0);
$input2 = WithSideEffects(Ensure([<|y: 1|>], FALSE));

SELECT
    *
FROM (
    SELECT
        *
    FROM
        AS_TABLE($input1)
) AS a
JOIN (
    SELECT
        *
    FROM
        AS_TABLE($input2)
) AS b
ON
    a.x == b.y
;
