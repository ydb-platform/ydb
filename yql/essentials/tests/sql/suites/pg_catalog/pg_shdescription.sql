--!syntax_pg
select
objoid,
classoid,
description
from
pg_catalog.pg_shdescription
order by objoid
