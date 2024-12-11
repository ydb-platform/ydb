PRAGMA TablePathPrefix='/Root';

PRAGMA ydb.OptimizerHints = 
'
    Rows(R # 10e8)
    Rows(T # 1)
    Rows(R T # 1)
    Rows(R S # 10e8)
    Rows(T U # 10e8)
    Rows(V # 1)
    JoinOrder(T U)
    JoinOrder(R S)
';

SELECT * FROM 
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id
        INNER JOIN  V   on  U.id = V.id;
