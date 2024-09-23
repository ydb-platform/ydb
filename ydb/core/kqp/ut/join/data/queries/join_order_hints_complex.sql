PRAGMA TablePathPrefix='/Root';

PRAGMA ydb.OptimizerHints = 
'
    Card(Unused # 10e8)
    JoinOrder( (Unused1 Unused2) (Unused3 Unused4) )

    Card(R # 10e8)
    Card(T # 1)
    Card(R T # 1)
    Card(R S # 10e8)
    Card(T U # 10e8)
    Card(V # 1)
    JoinOrder( (R S) (T U) )
';

SELECT * FROM 
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id
        INNER JOIN  V   on  U.id = V.id;
