/* postgres can not */
SELECT
    Date('2001-01-01') == Datetime('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') == Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Datetime('2001-01-01T00:00:00Z') == Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') != Datetime('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') != Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Datetime('2001-01-01T00:00:00Z') != Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') < Datetime('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') < Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Datetime('2001-01-01T00:00:00Z') < Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') <= Datetime('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') <= Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Datetime('2001-01-01T00:00:00Z') <= Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') > Datetime('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') > Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Datetime('2001-01-01T00:00:00Z') > Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') >= Datetime('2001-01-01T00:00:00Z')
;

SELECT
    Date('2001-01-01') >= Timestamp('2001-01-01T00:00:00Z')
;

SELECT
    Datetime('2001-01-01T00:00:00Z') >= Timestamp('2001-01-01T00:00:00Z')
;
