PRAGMA YqlSelect = 'force';

SELECT
    a,
    Count(*) AS count
FROM (
    VALUES
        (1),
        (1),
        (2)
) AS x (
    a
)
GROUP COMPACT BY
    GROUPING SETS (
        (a),
        ()
    )
ORDER BY
    a
;
