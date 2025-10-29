--!syntax_pg
select nullif(1, 1), nullif(1, 2.2), nullif(2, 1), nullif(1.2, '7'), nullif(1.2, '1.2');