/* custom error:Uncompatible member score types: Int32 and String */
$a = [
    <|score: 73|>
];

$b = [
    <|score: '42'|>
];

SELECT
    *
FROM
    AS_TABLE($a)
UNION ALL
SELECT
    *
FROM
    AS_TABLE($b)
;
