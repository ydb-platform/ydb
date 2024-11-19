/* postgres can not */
select Yql::Convert(Yql::Date(AsAtom("1")),AsAtom("Uint8"));
select Yql::Convert(Yql::Date(AsAtom("1")),AsAtom("Uint32"));
select Yql::Convert(Yql::Date(AsAtom("1")),AsAtom("Int32"));
select Yql::Convert(Yql::Date(AsAtom("1")),AsAtom("Uint64"));
select Yql::Convert(Yql::Date(AsAtom("1")),AsAtom("Int64"));

select Yql::Convert(Yql::Datetime(AsAtom("1")),AsAtom("Uint8"));
select Yql::Convert(Yql::Datetime(AsAtom("1")),AsAtom("Uint32"));
select Yql::Convert(Yql::Datetime(AsAtom("1")),AsAtom("Int32"));
select Yql::Convert(Yql::Datetime(AsAtom("1")),AsAtom("Uint64"));
select Yql::Convert(Yql::Datetime(AsAtom("1")),AsAtom("Int64"));

select Yql::Convert(Yql::Timestamp(AsAtom("1")),AsAtom("Uint8"));
select Yql::Convert(Yql::Timestamp(AsAtom("1")),AsAtom("Uint32"));
select Yql::Convert(Yql::Timestamp(AsAtom("1")),AsAtom("Int32"));
select Yql::Convert(Yql::Timestamp(AsAtom("1")),AsAtom("Uint64"));
select Yql::Convert(Yql::Timestamp(AsAtom("1")),AsAtom("Int64"));

select Yql::Convert(Yql::Interval(AsAtom("1")),AsAtom("Uint8"));
select Yql::Convert(Yql::Interval(AsAtom("1")),AsAtom("Uint32"));
select Yql::Convert(Yql::Interval(AsAtom("1")),AsAtom("Int32"));
select Yql::Convert(Yql::Interval(AsAtom("1")),AsAtom("Uint64"));
select Yql::Convert(Yql::Interval(AsAtom("1")),AsAtom("Int64"));
