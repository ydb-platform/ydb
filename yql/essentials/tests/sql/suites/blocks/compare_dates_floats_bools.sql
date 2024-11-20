use plato;

insert into @tmp
select
   1 as key,

   2.0f as fl1,
   3.0f as fl2,

   2.5  as db1,
   1.5  as db2,
   
   true as b1,
   false as b2,
   
   Date('2023-01-08') as d1,
   Date('2023-01-05') as d2,
   
   Datetime('2023-01-08T00:00:00Z') as dt1,
   Datetime('2023-01-05T00:00:00Z') as dt2,

   Timestamp('2023-01-08T00:00:00.000000Z') as ts1,
   Timestamp('2023-01-05T00:00:00.000000Z') as ts2,
   
   Interval('PT1M') as i1,
   Interval('PT1H') as i2,
;
commit;

select
   t.*,

   fl1 > fl2,
   fl1 > key,
   fl2 > 2.0,
   fl2 > db2,
   
   db1 > db2,
   db2 <= key,
   db1 > fl2,
   db2 > 1.0f,
   
   b1 < b2,
   b1 == true,
   false >= b2,
   
   d1 < d2,
   d1 == dt1,
   ts2 == d2,
   d1 >= Date('2023-01-01'),
   ts1 == d1,
   ts2 >= dt2,
   Timestamp('2023-01-05T00:00:00.000000Z') <= d2,
   
   
   i1 < i2,
   i2 > Interval('PT59M'),
from @tmp as t;   

