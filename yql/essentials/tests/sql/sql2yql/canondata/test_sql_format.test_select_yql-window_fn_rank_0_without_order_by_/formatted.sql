PRAGMA YqlSelect = 'force';

SELECT
    Rank() OVER w,
    DenseRank() OVER w,
    PercentRank() OVER w
FROM
    AS_TABLE([<|key: 1|>])
WINDOW
    w AS ()
;
