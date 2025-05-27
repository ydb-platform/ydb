PRAGMA DisableSimpleColumns;

/* postgres can not */
$input = (
    SELECT
        2 AS id,
        3 AS taskId,
        4 AS previousId
    UNION ALL
    SELECT
        1 AS id,
        NULL AS taskId,
        2 AS previousId
);

SELECT
    count(*)
FROM
    $input AS diff
INNER JOIN
    $input AS taskSuite
ON
    diff.previousId == taskSuite.id
LEFT JOIN
    $input AS pedestrian
ON
    diff.taskId == pedestrian.id
WHERE
    diff.id == 1
;
