PRAGMA YqlSelect = 'force';

SELECT
    a,
    b,
    Count(z)
FROM (
    VALUES
        (1, CAST(11 AS Int32?), 11),
        (2, CAST(21 AS Int32?), 21),
        (2, CAST(22 AS Int32?), 22),
        (3, CAST(31 AS Int32?), 31),
        (3, CAST(32 AS Int32?), 32),
        (3, CAST(33 AS Int32?), 33)
) AS x (
    a,
    b,
    z
)
GROUP BY
    GROUPING SETS (
        (a, b),
        (a),
        ()
    )
;
