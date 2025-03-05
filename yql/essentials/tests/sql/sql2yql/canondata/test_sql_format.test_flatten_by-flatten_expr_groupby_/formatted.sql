/* syntax version 1 */
/* postgres can not */
$data = (
    SELECT
        'a,b,c,d' AS a,
        'e,f,g,h' AS b,
        'x' AS c
);

SELECT
    bb,
    count(*) AS count
FROM
    $data
    FLATTEN BY (
        String::SplitToList(a, ',') AS a,
        String::SplitToList(b, ',') AS bb
    )
GROUP BY
    bb
ORDER BY
    bb,
    count
;
