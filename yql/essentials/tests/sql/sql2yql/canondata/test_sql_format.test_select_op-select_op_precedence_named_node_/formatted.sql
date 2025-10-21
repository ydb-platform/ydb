$t1 = (
    (
        SELECT
            *
        FROM (
            VALUES
                (1),
                (2)
        ) AS t (
            x
        )
    )
    UNION
    (
        SELECT
            *
        FROM (
            VALUES
                (2),
                (3)
        ) AS t (
            x
        )
    )
    INTERSECT
    (
        SELECT
            *
        FROM (
            VALUES
                (3)
        ) AS t (
            x
        )
    )
);

$t2 = (
    (
        SELECT
            *
        FROM (
            VALUES
                (3)
        ) AS t (
            x
        )
    )
    UNION
    (
        SELECT
            *
        FROM (
            VALUES
                (2)
        ) AS t (
            x
        )
    )
    EXCEPT
    (
        SELECT
            *
        FROM (
            VALUES
                (3)
        ) AS t (
            x
        )
    )
);

$t3 = (
    (
        SELECT
            *
        FROM (
            VALUES
                (1)
        ) AS t (
            x
        )
    )
    UNION
    (
        SELECT
            *
        FROM (
            VALUES
                (2)
        ) AS t (
            x
        )
    )
    INTERSECT
    (
        SELECT
            *
        FROM (
            VALUES
                (2),
                (3)
        ) AS t (
            x
        )
    )
    EXCEPT
    (
        SELECT
            *
        FROM (
            VALUES
                (3)
        ) AS t (
            x
        )
    )
    UNION
    (
        SELECT
            *
        FROM (
            VALUES
                (4),
                (3)
        ) AS t (
            x
        )
    )
    EXCEPT
    (
        SELECT
            *
        FROM (
            VALUES
                (4)
        ) AS t (
            x
        )
    )
);

SELECT
    *
FROM
    $t1
;

SELECT
    *
FROM
    $t2
;

SELECT
    *
FROM
    $t3
;
