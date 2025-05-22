/* postgres can not */
SELECT
    CAST(Utf8('true') AS bool),
    CAST(Utf8('-1') AS Int32),
    CAST(Utf8('-3.5') AS Double),
    CAST(Utf8('P1D') AS Interval),
    CAST(Utf8('2000-01-01') AS Date),
    CAST(Utf8('2000-01-01,GMT') AS TzDate)
;
