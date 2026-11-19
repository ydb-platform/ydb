PRAGMA YqlSelect = 'force';

$a = 'a';
$x = <|a: 7|>;

SELECT
    x.a AS member,
    x.$a AS named_member,
    $x.a AS named_value,
    $x.$a AS named_value_member,
    t.0 AS tuple_index,
    x.`0` AS quoted_index,
    s.x.a AS qualified_member
FROM
    AS_TABLE([<|x: <|a: 1, `0`: 2|>, a: <|a: 2|>, t: (3, 4)|>]) AS s
;

SELECT
    x.$a.b AS chain,
    (x.$a).b AS parenthesized_chain
FROM
    AS_TABLE([<|x: <|a: <|b: 11|>|>, a: <|a: <|b: 22|>|>|>])
;

SELECT
    x.$a AS member,
    v
FROM
    AS_TABLE([<|x: <|a: 1|>, a: <|a: [2]|>|>]) AS l
    FLATTEN LIST BY (
        x.$a AS v
    )
CROSS JOIN
    AS_TABLE([<|k: 0|>]) AS r
;
