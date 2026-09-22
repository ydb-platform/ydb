PRAGMA YqlSelect = 'force';

SELECT
    Rank(key) OVER w
FROM
    AS_TABLE([<|key: 1|>])
WINDOW
    w AS ()
;
