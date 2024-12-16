/* postgres can not */
select * from (
select 1 as a,Uuid('00000000-0000-0000-0000-000000000000') as x
union all
select 2 as a,Uuid('01000000-0000-0000-0000-000000000000') as x
union all
select 3 as a,Uuid('00010000-0000-0000-0000-000000000000') as x
union all
select 4 as a,Uuid('00000100-0000-0000-0000-000000000000') as x
union all
select 5 as a,Uuid('00000001-0000-0000-0000-000000000000') as x
union all
select 6 as a,Uuid('00000000-0100-0000-0000-000000000000') as x
union all
select 7 as a,Uuid('00000000-0001-0000-0000-000000000000') as x
union all
select 8 as a,Uuid('00000000-0000-0100-0000-000000000000') as x
union all
select 9 as a,Uuid('00000000-0000-0001-0000-000000000000') as x
union all
select 10 as a,Uuid('00000000-0000-0000-0100-000000000000') as x
union all
select 11 as a,Uuid('00000000-0000-0000-0001-000000000000') as x
union all
select 12 as a,Uuid('00000000-0000-0000-0000-010000000000') as x
union all
select 13 as a,Uuid('00000000-0000-0000-0000-000100000000') as x
union all
select 14 as a,Uuid('00000000-0000-0000-0000-000001000000') as x
union all
select 15 as a,Uuid('00000000-0000-0000-0000-000000010000') as x
union all
select 16 as a,Uuid('00000000-0000-0000-0000-000000000100') as x
union all
select 17 as a,Uuid('00000000-0000-0000-0000-000000000001') as x
) as s order by x;
