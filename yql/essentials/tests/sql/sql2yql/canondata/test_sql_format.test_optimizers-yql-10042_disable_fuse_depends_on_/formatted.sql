/* postgres can not */
$data = AsList((1 AS a, 1 AS b));

SELECT
    RandomNumber(a),
    RandomNumber(b)
FROM
    AS_TABLE($data)
;
