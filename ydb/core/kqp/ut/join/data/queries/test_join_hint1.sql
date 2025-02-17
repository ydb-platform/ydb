PRAGMA ydb.OptimizerHints = 
'
    JoinType(R S Shuffle) 
    Rows(R # 1) 
    Rows(R S # 10e6)
';

SELECT *
FROM `/Root/R` as R
    INNER JOIN
        `/Root/S` as S
    ON R.id = S.id