--!syntax_pg
select oid,
typinput::int4 as typinput,
typname,
typnamespace,
typtype
from pg_catalog.pg_type
order by oid

