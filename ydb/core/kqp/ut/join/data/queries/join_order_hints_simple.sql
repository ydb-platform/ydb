PRAGMA TablePathPrefix='/Root';

PRAGMA ydb.OptimizerHints = 
'
    Card(R # 10e8) 
    Card(T # 1) 
    Card(S # 10e8) 
    Card(R T # 1) 
    Card(R S # 10e8) 
    JoinOrder(T (R S))
';

SELECT * FROM 
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id