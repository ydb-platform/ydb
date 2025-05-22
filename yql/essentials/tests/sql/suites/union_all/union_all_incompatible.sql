select key from
(
    select key, subkey from
    (
        select 1 as key, "foo" as subkey
        union all
        select 2 as key, "bar" as subkey
        union all
        select 3 as key, 123 as subkey
    )
)
order by key;


select key from
(
    select * from
    (
        select 4 as key, "baz" as subkey
        union all
        select 5 as key, "goo" as subkey
        union all
        select 6 as key, 456 as subkey
    )
)
order by key;
