/* postgres can not */
SELECT
    CAST(date('1970-01-02') - date('1970-01-01') AS string)
;

SELECT
    CAST(date('1970-01-01') + interval('P1D') AS string)
;

SELECT
    CAST(interval('P1D') + date('1970-01-01') AS string)
;

SELECT
    CAST(date('1970-01-02') - interval('P1D') AS string)
;

SELECT
    CAST(datetime('1970-01-02T00:00:00Z') - datetime('1970-01-01T00:00:00Z') AS string)
;

SELECT
    CAST(datetime('1970-01-01T00:00:00Z') + interval('P1D') AS string)
;

SELECT
    CAST(interval('P1D') + datetime('1970-01-01T00:00:00Z') AS string)
;

SELECT
    CAST(datetime('1970-01-02T00:00:00Z') - interval('P1D') AS string)
;

SELECT
    CAST(timestamp('1970-01-02T00:00:00.6Z') - timestamp('1970-01-01T00:00:00.3Z') AS string)
;

SELECT
    CAST(timestamp('1970-01-01T00:00:00.6Z') + interval('P1D') AS string)
;

SELECT
    CAST(interval('P1D') + timestamp('1970-01-01T00:00:00.6Z') AS string)
;

SELECT
    CAST(timestamp('1970-01-02T00:00:00.6Z') - interval('P1D') AS string)
;

SELECT
    CAST(interval('P1D') + interval('P1D') AS string)
;

SELECT
    CAST(interval('P1D') - interval('P1D') AS string)
;

SELECT
    CAST(interval('P1D') * 2l AS string)
;

SELECT
    CAST(2u * interval('P1D') AS string)
;

SELECT
    CAST(interval('P1D') / 2 AS string)
;

SELECT
    CAST(interval('P1D') / 0ut AS string)
;
