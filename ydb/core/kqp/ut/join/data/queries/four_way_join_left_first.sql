SELECT *
FROM `/Root/R` as R
    LEFT JOIN
        `/Root/S` as S
    ON R.id = S.id
    INNER JOIN
        `/Root/T` as T
    ON S.id = T.id
    INNER JOIN
        `/Root/U` as U
    ON T.id = U.id
    INNER JOIN
        `/Root/V` as V
    ON U.id = V.id;
