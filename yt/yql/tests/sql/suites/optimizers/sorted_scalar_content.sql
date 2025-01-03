use plato;

$ou = select * from Input;

$a = select key from $ou where key > '0';

insert into @a
select * from $a order by key;

select * from $ou
where subkey > "0"
    and key != $a
order by key;