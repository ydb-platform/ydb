use plato;

select subkey from Input order by key, value;
select subkey from Input order by "x" || key, value;
select subkey from Input order by key || "x" limit 3;

select subkey from Input as a order by "x" || key, a.value limit 3;
select subkey from Input as a order by a.key, value limit 1;
select subkey from Input as a order by key, value limit 2;

select subkey, key from Input order by key, value;
