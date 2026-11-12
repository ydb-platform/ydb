PRAGMA YqlSelect = 'force';
PRAGMA AnsiCurrentRow;

SELECT
    x,
    RowNumber() OVER w AS rn,
    CumeDist() OVER w AS cd,
    sum(x) OVER w AS s
FROM
    AS_TABLE([<|x: 1|>, <|x: 1|>, <|x: 2|>])
WINDOW
    w AS (
        ORDER BY
            x
    )
ORDER BY
    x,
    rn
;
