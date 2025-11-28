USE plato;

$sub = (
    select key
    from Input
    flatten list by
        key
);

insert into Output
    select
        value,
        ListFilter(
            [value],
            ($x) -> ($x in $sub)
        ) as f
    from Input
    order by value
    