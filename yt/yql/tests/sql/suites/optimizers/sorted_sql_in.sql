use plato;

$ou = select * from Input;

$a = select * from $ou where key > '0';

insert into @a
select * from $a order by key;

select * from $ou
where subkey > "0"
    and key not in compact (select key from $a)
order by key;