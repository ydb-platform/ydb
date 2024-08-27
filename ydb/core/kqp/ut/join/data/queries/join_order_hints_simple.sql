PRAGMA TablePathPrefix='/Root';

PRAGMA ydb.OptCardinalityHints = 
    '[
        {"labels":["R"], "op":"#", "value":10e8},
        {"labels":["T"], "op":"#", "value":1},
        {"labels":["S"], "op":"#", "value":10e8},
        {"labels":["R", "T"], "op":"#", "value":1},
        {"labels":["R", "S"], "op":"#", "value":10e8}
    ]';
PRAGMA ydb.OptJoinOrderHints='[ "T", ["R", "S"] ]';

SELECT * FROM 
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id