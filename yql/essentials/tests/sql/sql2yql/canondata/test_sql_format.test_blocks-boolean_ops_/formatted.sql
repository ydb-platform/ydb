USE plato;

SELECT
    key,
    b1 AND b2 AS abb,
    b2 AND ob1 AS abo,
    ob2 AND b1 AS aob,
    ob1 AND ob2 AS aoo,
    b1 OR b2 AS obb,
    b2 OR ob1 AS obo,
    ob2 OR b1 AS oob,
    ob1 OR ob2 AS ooo,
    b1 XOR b2 AS xbb,
    b2 XOR ob1 AS xbo,
    ob2 XOR b1 AS xob,
    ob1 XOR ob2 AS xoo,
    (1 > 2) AND b1 AND b2 AND ob1 AND ob2 AS chain1,
    (1 < 2) XOR b1 XOR b2 XOR ob1 XOR ob2 AS chain2,
    (1 / 0 < 1) OR b1 OR b2 OR ob1 OR ob2 AS chain3,
FROM
    Input
ORDER BY
    key
;
