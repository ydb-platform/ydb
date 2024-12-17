/* postgres can not */
USE plato;

SELECT
    Yql::Date(AsAtom("1")),
    Yql::Datetime(AsAtom("2")),
    Yql::Timestamp(AsAtom("3")),
    Yql::Interval(AsAtom("4"))
FROM Input;

SELECT
    Yql::Date(AsAtom("1")),
    Yql::Datetime(AsAtom("2")),
    Yql::Timestamp(AsAtom("3")),
    Yql::Interval(AsAtom("4"));
