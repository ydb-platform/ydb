/* syntax version 1 */
/* postgres can not */
USE plato;

insert into @ksv
select * from Input order by key, subkey, value;

insert into @vsk
select * from Input order by value, subkey, key;

insert into @vs
select * from Input order by value, subkey;

commit;

select key, subkey, value from @ksv -- YtReduce
group compact by key, subkey, value
order by key, subkey, value;

select key, subkey, value from @vsk -- YtReduce
group /*+ compact() */ by key, subkey, value
order by key, subkey, value;

select key, subkey, some(value) as value from @ksv -- YtReduce
group compact by key, subkey
order by key, subkey, value;

select key, subkey, some(value) as value from @vsk -- YtMapReduce
group compact by key, subkey
order by key, subkey, value;

select key, subkey, value from concat(@ksv, @vsk) -- YtMapReduce
group compact by key, subkey, value
order by key, subkey, value;

select some(key) as key, subkey, value from concat(@vs, @vsk) -- YtReduce
group compact by subkey, value
order by key, subkey, value;
