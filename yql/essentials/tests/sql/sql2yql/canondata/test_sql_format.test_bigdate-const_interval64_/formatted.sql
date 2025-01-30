SELECT
    interval64('-P106751616DT23H59M59.999999S'),
    CAST(interval64('-P106751616DT23H59M59.999999S') AS string)
;

SELECT
    interval64('P106751616DT23H59M59.999999S'),
    CAST(interval64('P106751616DT23H59M59.999999S') AS string)
;

SELECT
    interval64('P106709244DT999999H999999M999999.999999S'),
    CAST(interval64('P106709244DT999999H999999M999999.999999S') AS string)
;

SELECT
    interval64('P000000000DT00H00M00.000000S'),
    CAST(interval64('P000000000DT00H00M00.000000S') AS string)
;

SELECT
    interval64('PT0S'),
    CAST(interval64('PT0S') AS string)
;

SELECT
    interval64('-PT0S'),
    CAST(interval64('-PT0S') AS string)
;

SELECT
    interval64('PT0.000001S'),
    CAST(interval64('PT0.000001S') AS string)
;

SELECT
    interval64('-PT0.000001S'),
    CAST(interval64('-PT0.000001S') AS string)
;

SELECT
    interval64('PT0S'),
    CAST(interval64('PT0S') AS string)
;

SELECT
    interval64('PT0M'),
    CAST(interval64('PT0M') AS string)
;

SELECT
    interval64('PT0H'),
    CAST(interval64('PT0H') AS string)
;

SELECT
    interval64('P0D'),
    CAST(interval64('P0D') AS string)
;

SELECT
    interval64('P0W'),
    CAST(interval64('P0W') AS string)
;

SELECT
    interval64('PT999999S'),
    CAST(interval64('PT999999S') AS string)
;

SELECT
    interval64('PT999999M'),
    CAST(interval64('PT999999M') AS string)
;

SELECT
    interval64('PT999999H'),
    CAST(interval64('PT999999H') AS string)
;

SELECT
    interval64('P106751616D'),
    CAST(interval64('P106751616D') AS string)
;

SELECT
    interval64('P15250230W'),
    CAST(interval64('P15250230W') AS string)
;

SELECT
    interval64('PT1S'),
    CAST(interval64('PT1S') AS string)
;

SELECT
    interval64('PT1M'),
    CAST(interval64('PT1M') AS string)
;

SELECT
    interval64('PT1H'),
    CAST(interval64('PT1H') AS string)
;

SELECT
    interval64('P1D'),
    CAST(interval64('P1D') AS string)
;

SELECT
    interval64('P1W'),
    CAST(interval64('P1W') AS string)
;

SELECT
    interval64('-PT1S'),
    CAST(interval64('-PT1S') AS string)
;

SELECT
    interval64('-PT1M'),
    CAST(interval64('-PT1M') AS string)
;

SELECT
    interval64('-PT1H'),
    CAST(interval64('-PT1H') AS string)
;

SELECT
    interval64('-P1D'),
    CAST(interval64('-P1D') AS string)
;

SELECT
    interval64('-P1W'),
    CAST(interval64('-P1W') AS string)
;
