PRAGMA YqlSelect = 'force';

WITH
    edges AS (
        SELECT
            src,
            dst
        FROM (
            VALUES
                (1, 2),
                (1, 3),
                (2, 3),
                (3, 4)
        ) AS x (
            src,
            dst
        )),
    RECURSIVE paths AS (
        SELECT
            1 AS node,
            0 AS depth
        UNION ALL
        SELECT
            e.dst AS node,
            p.depth + 1 AS depth
        FROM
            edges AS e
        INNER JOIN
            paths AS p
        ON
            e.src == p.node
    )
SELECT
    node,
    Min(depth)
FROM
    paths
GROUP BY
    node
ORDER BY
    Min(depth)
;
