/* postgres can not */
/* multirun can not */
use plato;

insert into @tmpTable
select date32('1969-12-31') as d32, datetime64('1969-12-31T0:0:0Z') as dt64, timestamp64('1969-12-31T0:0:0Z') as ts64, interval64('P65536D') as i64;

commit;

insert into Output
select * from @tmpTable where d32 < date32('1970-1-1');
