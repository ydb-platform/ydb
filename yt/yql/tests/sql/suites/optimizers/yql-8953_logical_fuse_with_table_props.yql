use plato;

select key, subkey, TableName() as name from Input
where value == 'q';

select key, count(*) as subkeys from (
    select distinct key, subkey from Input
    where value == 'q'
)
group by key;