--!syntax_pg
select 
roleid,
member,
grantor,
admin_option
from pg_catalog.pg_auth_members

