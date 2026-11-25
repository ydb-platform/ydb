PRAGMA YqlSelect = 'force';

SELECT
    y.a IS NULL AS missing
FROM
    AS_TABLE([<|y: Yson::Parse('{a=1;}')|>])
;
