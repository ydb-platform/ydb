/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

$input1 = ListTake([<|x: 1|>], 0);
$input2 = Yql::WithSideEffectsMode(Ensure([<|y: 1|>], FALSE), AsAtom('General'));

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
