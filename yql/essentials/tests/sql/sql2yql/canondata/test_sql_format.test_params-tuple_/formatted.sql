/* syntax version 1 */
DECLARE $x1 AS Tuple<String, Int64?>;
DECLARE $x2 AS Tuple<String, Int64?>;

$a, $b = $x1;

SELECT
    $a,
    $b,
    $x2.0,
    $x2.1
;
