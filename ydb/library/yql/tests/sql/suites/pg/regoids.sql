--!syntax_pg

-- Tests for OID types
-- See https://www.postgresql.org/docs/14/datatype-oid.html

select oid, oid::regclass from pg_catalog.pg_class
where oid::regclass in ('pg_user', 'pg_group');

select oid, oid::regconfig from pg_catalog.pg_ts_config
where oid::regconfig in ('english', 'russian');

select oid, oid::regdictionary from pg_catalog.pg_ts_dict
where oid::regdictionary in ('irish_stem', 'italian_stem');

select oid, oid::regnamespace from pg_catalog.pg_namespace
where oid::regnamespace in ('public', 'information_schema');

/*
TODO: uncomment after YQL-18711
select oid, oid::regoperator from pg_catalog.pg_operator
where oid::regoperator in ('=(integer,integer)', '+(float4,float8)');
*/

/*
TODO: uncomment after YQL-18712
select oid, oid::regproc from pg_catalog.pg_proc
where oid::regproc in ('int4in', 'int4out');
*/

/*
TODO: uncomment after YQL-18711
select oid, oid::regprocedure from pg_catalog.pg_proc
where oid::regprocedure in ('namein(cstring)', 'nameout(name)');
*/

select oid, oid::regrole from pg_catalog.pg_authid
where oid::regrole in ('pg_read_all_data', 'pg_write_all_data');

/*
TODO: uncomment after YQL-18713
select oid, oid::regtype from pg_catalog.pg_type
where oid::regtype in ('boolean', 'char');
*/

