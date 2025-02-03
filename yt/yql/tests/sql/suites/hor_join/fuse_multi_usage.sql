USE plato;

$input = select key, some(subkey) as subkey, some(value) as value from Input group by key;

select key from $input where subkey > "0"
order by key;

insert into @a
select t.*, RandomNumber(TableRow()) as rnd from $input as t where value > "a";

insert into @b
select * from $input;