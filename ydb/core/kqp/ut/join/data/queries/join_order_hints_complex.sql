PRAGMA TablePathPrefix='/Root';

PRAGMA ydb.OptJoinOrderHints='[ ["R", "S"], ["T", "U"] ]';
PRAGMA ydb.OptCardinalityHints = 
    '[
        {"labels":["R"], "op":"#", "value":10e8},
        {"labels":["T"], "op":"#", "value":1},
        {"labels":["R", "T"], "op":"#", "value":1},
        {"labels":["R", "S"], "op":"#", "value":10e8},
        {"labels":["T", "U"], "op":"#", "value":10e8},
        {"labels":["V"], "op":"#", "value":1}
    ]';

SELECT * FROM 
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id
        INNER JOIN  V   on  U.id = V.id;
