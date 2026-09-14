USE plato;

INSERT INTO Output
select 1 as key, Just(Just("x")) as subkey,
union all
select 2 as key, null as subkey,
union all
select 2 as key, Just(Nothing(String?)) as subkey,
ORDER BY key, subkey;
commit;


select key, subkey from Output where key == 2;
