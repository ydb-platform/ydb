/* postgres can not */
select cast(Yql::Date(AsAtom("1")) as Uint8);
select cast(Yql::Date(AsAtom("1")) as Uint32);
select cast(Yql::Date(AsAtom("1")) as Int32);
select cast(Yql::Date(AsAtom("1")) as Uint64);
select cast(Yql::Date(AsAtom("1")) as Int64);

select cast(Yql::Datetime(AsAtom("1")) as Uint8);
select cast(Yql::Datetime(AsAtom("1")) as Uint32);
select cast(Yql::Datetime(AsAtom("1")) as Int32);
select cast(Yql::Datetime(AsAtom("1")) as Uint64);
select cast(Yql::Datetime(AsAtom("1")) as Int64);

select cast(Yql::Timestamp(AsAtom("1")) as Uint8);
select cast(Yql::Timestamp(AsAtom("1")) as Uint32);
select cast(Yql::Timestamp(AsAtom("1")) as Int32);
select cast(Yql::Timestamp(AsAtom("1")) as Uint64);
select cast(Yql::Timestamp(AsAtom("1")) as Int64);

select cast(Yql::Interval(AsAtom("1")) as Uint8);
select cast(Yql::Interval(AsAtom("1")) as Uint32);
select cast(Yql::Interval(AsAtom("1")) as Int32);
select cast(Yql::Interval(AsAtom("1")) as Uint64);
select cast(Yql::Interval(AsAtom("1")) as Int64);

select cast(1ut as Date);
select cast(1u as Date);
select cast(1 as Date);
select cast(1ul as Date);
select cast(1l as Date);
select cast(-1 as Date);
select cast(1/1 as Date);
select cast(-1/1 as Date);
/* postgres can not */
select cast(Yql::Date(AsAtom("1")) as Uint8);
select cast(Yql::Date(AsAtom("1")) as Uint32);
select cast(Yql::Date(AsAtom("1")) as Int32);
select cast(Yql::Date(AsAtom("1")) as Uint64);
select cast(Yql::Date(AsAtom("1")) as Int64);

select cast(Yql::Datetime(AsAtom("1")) as Uint8);
select cast(Yql::Datetime(AsAtom("1")) as Uint32);
select cast(Yql::Datetime(AsAtom("1")) as Int32);
select cast(Yql::Datetime(AsAtom("1")) as Uint64);
select cast(Yql::Datetime(AsAtom("1")) as Int64);

select cast(Yql::Timestamp(AsAtom("1")) as Uint8);
select cast(Yql::Timestamp(AsAtom("1")) as Uint32);
select cast(Yql::Timestamp(AsAtom("1")) as Int32);
select cast(Yql::Timestamp(AsAtom("1")) as Uint64);
select cast(Yql::Timestamp(AsAtom("1")) as Int64);

select cast(Yql::Interval(AsAtom("1")) as Uint8);
select cast(Yql::Interval(AsAtom("1")) as Uint32);
select cast(Yql::Interval(AsAtom("1")) as Int32);
select cast(Yql::Interval(AsAtom("1")) as Uint64);
select cast(Yql::Interval(AsAtom("1")) as Int64);

select cast(1ut as Date);
select cast(1u as Date);
select cast(1 as Date);
select cast(1ul as Date);
select cast(1l as Date);
select cast(-1 as Date);
select cast(1/1 as Date);
select cast(-1/1 as Date);
select cast(1/0 as Date);

select cast(1ut as Datetime);
select cast(1u as Datetime);
select cast(1 as Datetime);
select cast(1ul as Datetime);
select cast(1l as Datetime);
select cast(-1 as Datetime);
select cast(1/1 as Datetime);
select cast(-1/1 as Datetime);
select cast(1/0 as Datetime);

select cast(1ut as Timestamp);
select cast(1u as Timestamp);
select cast(1 as Timestamp);
select cast(1ul as Timestamp);
select cast(1l as Timestamp);
select cast(-1 as Timestamp);
select cast(1/1 as Timestamp);
select cast(-1/1 as Timestamp);
select cast(1/0 as Timestamp);

select cast(1ut as Interval);
select cast(1u as Interval);
select cast(-1 as Interval);
select cast(1ul as Interval);
select cast(-1l as Interval);
select cast(-1 as Interval);
select cast(1/1 as Interval);
select cast(-1/1 as Interval);
select cast(1/0 as Interval);
