/* syntax version 1 */
/* postgres can not */
USE plato;
pragma yt.UseNativeDescSort;

select key, subkey from Input1 -- YtReduce
group compact by key, subkey
order by key, subkey;

select key, subkey from Input1 -- YtReduce
group compact by subkey, key
order by subkey, key;

select key from Input1 -- YtReduce
group compact by key
order by key;

select subkey from Input1 -- YtMapReduce
group compact by subkey
order by subkey;

select key, subkey from concat(Input1, Input2) -- YtMapReduce, mix of ascending/descending
group compact by key, subkey
order by key, subkey;
