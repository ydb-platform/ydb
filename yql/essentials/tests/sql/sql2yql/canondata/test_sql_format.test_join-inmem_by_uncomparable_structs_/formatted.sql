/* syntax version 1 */
$l = [
    <|Key: <|a: 1, b: 2, c: NULL|>, Lhs: '1,2,#'|>,
    <|Key: <|a: 2, b: 3, c: NULL|>, Lhs: '4,5,#'|>,
];

$r = [
    <|Key: <|a: 1, b: 2, c: 3|>, Rhs: '1,2,3'|>,
    <|Key: <|a: 4, b: 5, c: 6|>, Rhs: '4,5,6'|>,
];

SELECT
    Lhs,
    Rhs
FROM
    AS_TABLE($l) AS l
LEFT JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Lhs
FROM
    AS_TABLE($l) AS l
LEFT SEMI JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Lhs
FROM
    AS_TABLE($l) AS l
LEFT ONLY JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Rhs,
    Lhs
FROM
    AS_TABLE($l) AS l
RIGHT JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Rhs
FROM
    AS_TABLE($l) AS l
RIGHT SEMI JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Rhs
FROM
    AS_TABLE($l) AS l
RIGHT ONLY JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Lhs,
    Rhs
FROM
    AS_TABLE($l) AS l
INNER JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Lhs,
    Rhs
FROM
    AS_TABLE($l) AS l
FULL JOIN
    AS_TABLE($r) AS r
USING (Key);

SELECT
    Lhs,
    Rhs
FROM
    AS_TABLE($l) AS l
EXCLUSION JOIN
    AS_TABLE($r) AS r
USING (Key);
