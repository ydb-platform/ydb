PRAGMA YqlSelect = 'force';

-- TODO(YQL-20436): support named node.
SELECT
    student,
    grade
FROM (
    VALUES
        (5, 'Ivan'),
        (5, 'Lev'),
        (3, 'Aleksandr'),
        (4, 'Anastasia'),
        (2, 'Alexei'),
        (2, 'Dmitri'),
        (4, 'Maxim'),
        (5, 'Daniil')
) AS exam (
    grade,
    student
)
WHERE
    grade == (
        SELECT
            MIN(grade)
        FROM (
            VALUES
                (5, 'Ivan'),
                (5, 'Lev'),
                (3, 'Aleksandr'),
                (4, 'Anastasia'),
                (2, 'Alexei'),
                (2, 'Dmitri'),
                (4, 'Maxim'),
                (5, 'Daniil')
        ) AS exam (
            grade,
            student
        )
    )
;
