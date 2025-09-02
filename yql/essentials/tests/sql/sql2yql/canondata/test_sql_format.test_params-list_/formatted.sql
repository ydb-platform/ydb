/* syntax version 1 */
DECLARE $x AS List<String?>;

SELECT
    ListLength($x),
    $x[0],
    $x[1],
    $x[2],
    $x[3]
;
