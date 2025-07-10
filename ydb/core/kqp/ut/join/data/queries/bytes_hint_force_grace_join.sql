PRAGMA TablePathPrefix='/Root';

PRAGMA ydb.OptimizerHints =
'
    Rows(R # 0)
    Rows(S # 0)
    Bytes(R # 228e30)
    Bytes(S # 1337e30)
    Bytes(R S # 228e30)
';

SELECT * FROM
    R INNER JOIN S ON R.id = S.id