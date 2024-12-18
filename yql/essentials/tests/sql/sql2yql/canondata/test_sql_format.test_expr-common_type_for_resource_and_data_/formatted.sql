SELECT
    AsList(DateTime::StartOfMonth(CurrentUtcDate()), CurrentUtcDate())
;

SELECT
    AsList(Yson::Parse('1'), "2"y)
;

SELECT
    AsList(Yson::ParseJson('1'), "2"j)
;

SELECT
    AsList(Json2::Parse('1'), "2"j)
;
