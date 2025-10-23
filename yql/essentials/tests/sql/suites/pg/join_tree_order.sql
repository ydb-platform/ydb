--!syntax_pg
SELECT DISTINCT cs.oid
FROM pg_catalog.pg_attribute att
JOIN (pg_catalog.pg_depend JOIN pg_catalog.pg_class cs ON objid=cs.oid) ON refobjid=att.attrelid