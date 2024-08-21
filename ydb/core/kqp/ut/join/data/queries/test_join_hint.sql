PRAGMA ydb.OptCardinalityHints = '[{"labels":["R"], "op":"#", "value":1}, {"labels":["R","S"], "op":"#", "value":10e6}]';
PRAGMA ydb.OptJoinAlgoHints = '[{"labels":["R","S"], "algo":"GraceJoin"}]';

SELECT *
FROM `/Root/R` as R
    INNER JOIN
        `/Root/S` as S
    ON R.id = S.id