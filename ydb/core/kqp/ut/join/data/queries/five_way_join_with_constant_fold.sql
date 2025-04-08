SELECT R1.id
    FROM `/Root/S` as S
    INNER JOIN `/Root/R` as R1 ON R1.id = S.id
    INNER JOIN `/Root/R` as R2 ON R2.id = S.id