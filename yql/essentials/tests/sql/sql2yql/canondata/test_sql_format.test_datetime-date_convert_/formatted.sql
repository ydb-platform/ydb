/* postgres can not */
SELECT
    Yql::Convert(Yql::Date(AsAtom('1')), AsAtom('Uint8'))
;

SELECT
    Yql::Convert(Yql::Date(AsAtom('1')), AsAtom('Uint32'))
;

SELECT
    Yql::Convert(Yql::Date(AsAtom('1')), AsAtom('Int32'))
;

SELECT
    Yql::Convert(Yql::Date(AsAtom('1')), AsAtom('Uint64'))
;

SELECT
    Yql::Convert(Yql::Date(AsAtom('1')), AsAtom('Int64'))
;

SELECT
    Yql::Convert(Yql::Datetime(AsAtom('1')), AsAtom('Uint8'))
;

SELECT
    Yql::Convert(Yql::Datetime(AsAtom('1')), AsAtom('Uint32'))
;

SELECT
    Yql::Convert(Yql::Datetime(AsAtom('1')), AsAtom('Int32'))
;

SELECT
    Yql::Convert(Yql::Datetime(AsAtom('1')), AsAtom('Uint64'))
;

SELECT
    Yql::Convert(Yql::Datetime(AsAtom('1')), AsAtom('Int64'))
;

SELECT
    Yql::Convert(Yql::Timestamp(AsAtom('1')), AsAtom('Uint8'))
;

SELECT
    Yql::Convert(Yql::Timestamp(AsAtom('1')), AsAtom('Uint32'))
;

SELECT
    Yql::Convert(Yql::Timestamp(AsAtom('1')), AsAtom('Int32'))
;

SELECT
    Yql::Convert(Yql::Timestamp(AsAtom('1')), AsAtom('Uint64'))
;

SELECT
    Yql::Convert(Yql::Timestamp(AsAtom('1')), AsAtom('Int64'))
;

SELECT
    Yql::Convert(Yql::Interval(AsAtom('1')), AsAtom('Uint8'))
;

SELECT
    Yql::Convert(Yql::Interval(AsAtom('1')), AsAtom('Uint32'))
;

SELECT
    Yql::Convert(Yql::Interval(AsAtom('1')), AsAtom('Int32'))
;

SELECT
    Yql::Convert(Yql::Interval(AsAtom('1')), AsAtom('Uint64'))
;

SELECT
    Yql::Convert(Yql::Interval(AsAtom('1')), AsAtom('Int64'))
;
