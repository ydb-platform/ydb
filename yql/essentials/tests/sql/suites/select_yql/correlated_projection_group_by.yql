PRAGMA YqlSelect = 'force';

SELECT
    student.name AS student,
    (
        SELECT
            AVG(grade)
        FROM (
            VALUES
                (5, 'Ivan'),
                (5, 'Ivan'),
                (5, 'Ivan'),
                (4, 'Lev'),
                (5, 'Lev'),
                (4, 'Lev'),
                (3, 'Maxim'),
                (2, 'Maxim'),
                (3, 'Maxim')
        ) AS exam (
            grade,
            student
        )
        WHERE
            student.name == exam.student
    ) AS avg_grade
FROM (
    VALUES
        ('Ivan'),
        ('Lev'),
        ('Maxim')
) AS student (
    name
);
