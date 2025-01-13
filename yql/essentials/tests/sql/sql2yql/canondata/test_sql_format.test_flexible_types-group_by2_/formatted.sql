/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

$groupsrc = (
    SELECT
        '1' AS key,
        '2' AS String
    UNION ALL
    SELECT
        '1' AS key,
        '3' AS String
);

$foo = ($k, $t) -> (FormatType($t) || '_' || $k);

SELECT
    $foo(key, String)
FROM
    $groupsrc
GROUP BY
    key
;
