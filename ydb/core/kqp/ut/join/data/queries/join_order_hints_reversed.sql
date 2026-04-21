PRAGMA TablePathPrefix='/Root';

-- Without the hint the optimizer prefers to stream R (R is much larger than S).
-- The hint forces S to be the right (build) table.
PRAGMA ydb.OptimizerHints=
'
    Rows(S # 1000)
    Rows(R # 10000000000)
    JoinOrder(S R)
';

select * from R inner join S on R.id = S.id;
