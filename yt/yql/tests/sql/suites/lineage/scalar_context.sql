USE plato;

$sub = (
    select key
    from Input where value = 'abc'
);

insert into Output
    select
       value, $sub as s
    from Input
    order by value
