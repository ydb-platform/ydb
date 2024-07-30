/* postgres can not */
/* multirun can not */
use plato;

pragma yt.UseNewPredicateExtraction;

insert into OutDate32
select * from as_table(AsList(
    <|key:Date32('-144169-1-1')|>,
    <|key:Date32('148107-12-31')|>))
order by key;

insert into OutDatetime64
select * from as_table(AsList(
    <|key:Datetime64('-144169-1-1T0:0:0Z')|>,
    <|key:Datetime64('148107-12-31T23:59:59Z')|>))
order by key;

insert into OutTimestamp64
select * from as_table(AsList(
    <|key:Timestamp64('-144169-1-1T0:0:0Z')|>,
    <|key:Timestamp64('148107-12-31T23:59:59.999999Z')|>))
order by key;

commit;

select * from OutDate32
where key > Date('1970-1-1')
and key > Datetime('1970-1-1T0:0:0Z')
and key > Timestamp('1970-1-1T0:0:0Z')
and key > Date32('-144169-1-1')
and key > Datetime64('-144169-1-1T0:0:0Z')
and key > Timestamp64('-144169-1-1T0:0:0Z')
and key >= Date('2105-12-31')
and key >= Datetime('2105-12-31T23:59:59Z')
and key >= Timestamp('2105-12-31T23:59:59Z')
and key >= Date32('148107-12-31')
and key >= Datetime64('148107-12-31T0:0:0Z')
and key >= Timestamp64('148107-12-31T0:0:0Z')
;
select * from OutDate32
where key < Date('2105-12-31')
and key < Datetime('2105-12-31T23:59:59Z')
and key < Timestamp('2105-12-31T23:59:59.999999Z')
and key < Date32('148107-12-31')
and key < Datetime64('148107-12-31T23:59:59Z')
and key < Timestamp64('148107-12-31T23:59:59.999999Z')
and key <= Date('1970-1-1')
and key <= Datetime('1970-1-1T0:0:0Z')
and key <= Timestamp('1970-1-1T0:0:0Z')
and key <= Date32('-144169-1-1')
and key <= Datetime64('-144169-1-1T0:0:0Z')
and key <= Timestamp64('-144169-1-1T0:0:0Z')
;
select * from OutDatetime64
where key > Date('1970-1-1')
and key > Datetime('1970-1-1T0:0:0Z')
and key > Timestamp('1970-1-1T0:0:0Z')
and key > Date32('-144169-1-1')
and key > Datetime64('-144169-1-1T0:0:0Z')
and key > Timestamp64('-144169-1-1T0:0:0Z')
and key >= Date('2105-12-31')
and key >= Datetime('2105-12-31T23:59:59Z')
and key >= Timestamp('2105-12-31T23:59:59Z')
and key >= Date32('148107-12-31')
and key >= Datetime64('148107-12-31T0:0:0Z')
and key >= Timestamp64('148107-12-31T0:0:0Z')
;
select * from OutDatetime64
where key < Date('2105-12-31')
and key < Datetime('2105-12-31T23:59:59Z')
and key < Timestamp('2105-12-31T23:59:59.999999Z')
and key < Date32('148107-12-31')
and key < Datetime64('148107-12-31T23:59:59Z')
and key < Timestamp64('148107-12-31T23:59:59.999999Z')
and key <= Date('1970-1-1')
and key <= Datetime('1970-1-1T0:0:0Z')
and key <= Timestamp('1970-1-1T0:0:0Z')
and key <= Date32('-144169-1-1')
and key <= Datetime64('-144169-1-1T0:0:0Z')
and key <= Timestamp64('-144169-1-1T0:0:0Z')
;
select * from OutTimestamp64
where key > Date('1970-1-1')
and key > Datetime('1970-1-1T0:0:0Z')
and key > Timestamp('1970-1-1T0:0:0Z')
and key > Date32('-144169-1-1')
and key > Datetime64('-144169-1-1T0:0:0Z')
and key > Timestamp64('-144169-1-1T0:0:0Z')
and key >= Date('2105-12-31')
and key >= Datetime('2105-12-31T23:59:59Z')
and key >= Timestamp('2105-12-31T23:59:59Z')
and key >= Date32('148107-12-31')
and key >= Datetime64('148107-12-31T0:0:0Z')
and key >= Timestamp64('148107-12-31T0:0:0Z')
;
select * from OutTimestamp64
where key < Date('2105-12-31')
and key < Datetime('2105-12-31T23:59:59Z')
and key < Timestamp('2105-12-31T23:59:59.999999Z')
and key < Date32('148107-12-31')
and key < Datetime64('148107-12-31T23:59:59Z')
and key < Timestamp64('148107-12-31T23:59:59.999999Z')
and key <= Date('1970-1-1')
and key <= Datetime('1970-1-1T0:0:0Z')
and key <= Timestamp('1970-1-1T0:0:0Z')
and key <= Date32('-144169-1-1')
and key <= Datetime64('-144169-1-1T0:0:0Z')
and key <= Timestamp64('-144169-1-1T0:0:0Z')
;
