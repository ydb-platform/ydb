SELECT
    1,
    date32('-144169-01-01'),
    CAST(date32('-144169-01-01') AS string)
;

SELECT
    2,
    date32('-1-1-1'),
    CAST(date32('-1-1-1') AS string)
;

SELECT
    3,
    date32('1-1-1'),
    CAST(date32('1-1-1') AS string)
;

SELECT
    4,
    date32('1969-12-31'),
    CAST(date32('1969-12-31') AS string)
;

SELECT
    5,
    date32('1970-01-01'),
    CAST(date32('1970-01-01') AS string)
;

SELECT
    6,
    date32('2000-04-05'),
    CAST(date32('2000-04-05') AS string)
;

SELECT
    7,
    date32('2100-03-01'),
    CAST(date32('2100-03-01') AS string)
;

SELECT
    8,
    date32('2105-12-31'),
    CAST(date32('2105-12-31') AS string)
;

SELECT
    9,
    date32('2106-01-01'),
    CAST(date32('2106-01-01') AS string)
;

SELECT
    10,
    date32('148107-12-31'),
    CAST(date32('148107-12-31') AS string)
;
