/* syntax version 1 */
$l = AsList(
    AsStruct(AsTuple(1, 2, 3) AS Key, '1,2,3' AS Lhs),
    AsStruct(AsTuple(1, 2, 4) AS Key, '1,2,4' AS Lhs)
);

$r = AsList(
    AsStruct(AsTuple(1, 2) AS Key, '1,2' AS Rhs),
    AsStruct(AsTuple(2, 3) AS Key, '2,3' AS Rhs)
);

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
