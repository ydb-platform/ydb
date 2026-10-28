PRAGMA OrderedColumns;
PRAGMA YqlSelect = 'disable';

$left = AsList(AsStruct(1 AS key, 'a' AS x), AsStruct(2 AS key, 'b' AS x));
$right = AsList(AsStruct(2 AS key, 'c' AS y), AsStruct(3 AS key, 'd' AS y));

SELECT
    *
FROM
    AS_TABLE($left) AS a
FULL JOIN
    AS_TABLE($right) AS b
ON
    a.key == b.key
;
