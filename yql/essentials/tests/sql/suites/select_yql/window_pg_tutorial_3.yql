PRAGMA YqlSelect = 'force';

SELECT
    salary,
    Sum(salary) OVER ()
FROM (
    VALUES
        ('develop', 11, 5200),
        ('develop', 7, 4200),
        ('develop', 9, 4500),
        ('develop', 8, 6000),
        ('develop', 10, 5200),
        ('personne', 5, 3500),
        ('personne', 2, 3900),
        ('sales', 3, 4800),
        ('sales', 1, 5000),
        ('sales', 4, 4800)
) AS empsalary (
    depname,
    empno,
    salary
);
