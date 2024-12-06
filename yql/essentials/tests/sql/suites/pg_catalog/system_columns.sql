--!syntax_pg
select oid,tableoid,xmin,cmin,xmax,cmax,ctid from pg_type where typname = 'text';

