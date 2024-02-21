--!syntax_pg
select
constraint_schema,
table_name,
constraint_type
from
information_schema.table_constraints
order by constraint_schema, table_name

