PRAGMA YqlSelect = 'force';

SELECT
    depname,
    empno,
    salary,
    enroll_date
FROM (
    SELECT
        depname,
        empno,
        salary,
        enroll_date,
        RowNumber() OVER (
            PARTITION BY
                depname
            ORDER BY
                salary DESC,
                empno
        ) AS pos
    FROM (
        VALUES
            ('develop', 11, 5200, '2026-01-01'),
            ('develop', 7, 4200, '2026-01-01'),
            ('develop', 9, 4500, '2026-01-01'),
            ('develop', 8, 6000, '2026-01-01'),
            ('develop', 10, 5200, '2026-01-01'),
            ('personne', 5, 3500, '2026-01-01'),
            ('personne', 2, 3900, '2026-01-01'),
            ('sales', 3, 4800, '2026-01-01'),
            ('sales', 1, 5000, '2026-01-01'),
            ('sales', 4, 4800, '2026-01-01')
    ) AS empsalary (
        depname,
        empno,
        salary,
        enroll_date
    )
) AS ss
WHERE
    pos < 3
;
