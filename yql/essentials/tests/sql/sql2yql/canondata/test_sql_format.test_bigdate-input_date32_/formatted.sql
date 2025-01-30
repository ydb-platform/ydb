SELECT
    1,
    CAST('-144170-12-31' AS date32),
    CAST(CAST('-144170-12-31' AS date32) AS string)
;

SELECT
    2,
    CAST('-144169-01-01' AS date32),
    CAST(CAST('-144169-01-01' AS date32) AS string)
;

SELECT
    3,
    CAST('-1-1-1' AS date32),
    CAST(CAST('-1-1-1' AS date32) AS string)
;

SELECT
    4,
    CAST('0-1-1' AS date32),
    CAST(CAST('0-1-1' AS date32) AS string)
;

SELECT
    5,
    CAST('1-1-1' AS date32),
    CAST(CAST('1-1-1' AS date32) AS string)
;

SELECT
    6,
    CAST('1-02-29' AS date32),
    CAST(CAST('1-02-29' AS date32) AS string)
;

SELECT
    7,
    CAST('1969-12-31' AS date32),
    CAST(CAST('1969-12-31' AS date32) AS string)
;

SELECT
    8,
    CAST('1970-01-01' AS date32),
    CAST(CAST('1970-01-01' AS date32) AS string)
;

SELECT
    9,
    CAST('2000-04-05' AS date32),
    CAST(CAST('2000-04-05' AS date32) AS string)
;

SELECT
    10,
    CAST('2100-02-29' AS date32),
    CAST(CAST('2100-02-29' AS date32) AS string)
;

SELECT
    11,
    CAST('2100-03-01' AS date32),
    CAST(CAST('2100-03-01' AS date32) AS string)
;

SELECT
    12,
    CAST('2105-12-31' AS date32),
    CAST(CAST('2105-12-31' AS date32) AS string)
;

SELECT
    13,
    CAST('2106-01-01' AS date32),
    CAST(CAST('2106-01-01' AS date32) AS string)
;

SELECT
    14,
    CAST('148107-12-31' AS date32),
    CAST(CAST('148107-12-31' AS date32) AS string)
;

SELECT
    15,
    CAST('148108-01-01' AS date32),
    CAST(CAST('148108-01-01' AS date32) AS string)
;
