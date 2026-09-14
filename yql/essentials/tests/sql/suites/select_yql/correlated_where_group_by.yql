PRAGMA YqlSelect = 'force';

-- TODO(YQL-20436): support named node.
SELECT
    exam.course AS worst_course,
    exam.student AS student,
    grade
FROM (
    VALUES
        ('SE', 5, 'Ivan'),
        ('SE', 4, 'Lev'),
        ('SE', 3, 'Maxim'),
        ('OS', 5, 'Ivan'),
        ('OS', 5, 'Lev'),
        ('OS', 2, 'Maxim'),
        ('PL', 5, 'Ivan'),
        ('PL', 2, 'Lev'),
        ('PL', 4, 'Maxim')
) AS exam (
    course,
    grade,
    student
)
WHERE
    grade == (
        SELECT
            MIN(grade)
        FROM (
            VALUES
                ('SE', 5, 'Ivan'),
                ('SE', 4, 'Lev'),
                ('SE', 3, 'Maxim'),
                ('OS', 5, 'Ivan'),
                ('OS', 5, 'Lev'),
                ('OS', 2, 'Maxim'),
                ('PL', 5, 'Ivan'),
                ('PL', 2, 'Lev'),
                ('PL', 4, 'Maxim')
        ) AS exam1 (
            course,
            grade,
            student
        )
        WHERE
            exam.student == exam1.student
    )
;
