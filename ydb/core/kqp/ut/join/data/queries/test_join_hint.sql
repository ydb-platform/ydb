PRAGMA ydb.OptimizerHints = 
'
    JoinAlgo(R S Grace) 
    Card(R # 1) 
    Card(R S # 10e6)
';

SELECT *
FROM `/Root/R` as R
    INNER JOIN
        `/Root/S` as S
    ON R.id = S.id