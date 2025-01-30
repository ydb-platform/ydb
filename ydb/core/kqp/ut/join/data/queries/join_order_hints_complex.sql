PRAGMA TablePathPrefix='/Root';

PRAGMA ydb.OptimizerHints = 
'
    Rows(Unused # 10e8)
    JoinOrder( (Unused1 Unused2) (Unused3 Unused4) )

    Rows(R # 10e8)
    Rows(T # 1)
    Rows(R T # 1)
    Rows(R S # 10e8)
    Rows(T U # 10e8)
    Rows(V # 1)
    JoinOrder( (R S) (T U) )
    JoinType(T U Broadcast)
';

SELECT * FROM 
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id
        INNER JOIN  V   on  U.id = V.id;
