SELECT b.aa, COUNT(*) as cnt
FROM `/Root/a` as a
    INNER JOIN `/Root/b` as b
    ON a.kal = b.a
GROUP BY b.aa