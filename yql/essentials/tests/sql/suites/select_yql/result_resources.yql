PRAGMA YqlSelect = 'force';

SELECT
    AsList(DateTime::StartOfMonth(Date('2020-02-29')), Date('2020-02-29')) AS dates,
    Yson::Parse('1') AS present,
    Yson::Parse('[', Yson::Options(FALSE AS Strict)) AS missing
;
