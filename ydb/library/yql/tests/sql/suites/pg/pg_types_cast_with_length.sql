--!syntax_pg
select
    cast('foo' as varchar(2)),
    cast(12345 as varchar(2)),
    cast('{foo,bar}' as _varchar(2)),
    cast(array['foo','bar'] as _varchar(2)),
    cast(array[12345,67890] as _varchar(2)),
    cast(array['foo','bar'] as varchar(2)),
    cast(array[12345,67890] as varchar(2));