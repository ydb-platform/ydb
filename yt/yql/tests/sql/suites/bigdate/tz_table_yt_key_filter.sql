/* postgres can not */
/* multirun can not */
use plato;

pragma yt.UseNewPredicateExtraction;

insert into OutTzDate32
select * from as_table(AsList(
    <|key:TzDate32('-144169-1-1,UTC')|>,
    <|key:TzDate32('148107-12-31,UTC')|>))
order by key;

insert into OutTzDatetime64
select * from as_table(AsList(
    <|key:TzDatetime64('-144169-1-1T0:0:0,UTC')|>,
    <|key:TzDatetime64('148107-12-31T23:59:59,UTC')|>))
order by key;

insert into OutTzTimestamp64
select * from as_table(AsList(
    <|key:TzTimestamp64('-144169-1-1T0:0:0,UTC')|>,
    <|key:TzTimestamp64('148107-12-31T23:59:59.999999,UTC')|>))
order by key;

commit;

select * from OutTzDate32
where key > TzDate32('-144169-1-1,UTC')
and key > TzDatetime64('-144169-1-1T0:0:0,UTC')
and key > TzTimestamp64('-144169-1-1T0:0:0,UTC')
and key >= TzDate32('148107-12-31,UTC')
and key >= TzDatetime64('148107-12-31T0:0:0,UTC')
and key >= TzTimestamp64('148107-12-31T0:0:0,UTC')
;
select * from OutTzDate32
where key < TzDate32('148107-12-31,UTC')
and key < TzDatetime64('148107-12-31T23:59:59,UTC')
and key < TzTimestamp64('148107-12-31T23:59:59.999999,UTC')
and key <= TzDate32('-144169-1-1,UTC')
and key <= TzDatetime64('-144169-1-1T0:0:0,UTC')
and key <= TzTimestamp64('-144169-1-1T0:0:0,UTC')
;
select * from OutTzDatetime64
where key > TzDatetime64('-144169-1-1T0:0:0,UTC')
and key > TzTimestamp64('-144169-1-1T0:0:0,UTC')
and key >= TzDatetime64('148107-12-31T0:0:0,UTC')
and key >= TzTimestamp64('148107-12-31T0:0:0,UTC')
;
select * from OutTzDatetime64
where key < TzDatetime64('148107-12-31T23:59:59,UTC')
and key < TzTimestamp64('148107-12-31T23:59:59.999999,UTC')
and key <= TzDatetime64('-144169-1-1T0:0:0,UTC')
and key <= TzTimestamp64('-144169-1-1T0:0:0,UTC')
;
select * from OutTzTimestamp64
where key > TzTimestamp64('-144169-1-1T0:0:0,UTC')
and key >= TzTimestamp64('148107-12-31T0:0:0,UTC')
;
select * from OutTzTimestamp64
where key < TzTimestamp64('148107-12-31T23:59:59.999999,UTC')
and key <= TzTimestamp64('-144169-1-1T0:0:0,UTC')
;
