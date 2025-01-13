/* postgres can not */
/* syntax version 1 */
$data = AsList((1 AS a, 1 AS b));

SELECT
    RandomNumber(a),
    RandomNumber(b)
FROM
    AS_TABLE($data)
;
