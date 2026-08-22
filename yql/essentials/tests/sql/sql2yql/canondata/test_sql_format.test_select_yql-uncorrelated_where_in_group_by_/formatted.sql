PRAGMA YqlSelect = 'force';

SELECT
    exam.course AS course,
    exam.student AS worst_student,
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
    (course, grade) IN (
        SELECT
            (course, MIN(grade))
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
        GROUP BY
            course
    )
;
