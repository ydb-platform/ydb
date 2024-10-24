PRAGMA TablePathPrefix='/Root';
PRAGMA ydb.OptimizerHints = 
'
    JoinType(R S Shuffle)
    JoinType(R S T Broadcast)
    JoinType(R S T U Shuffle)
    JoinType(R S T U V Broadcast)
';

SELECT * FROM 
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id
        INNER JOIN  V   on  U.id = V.id;
