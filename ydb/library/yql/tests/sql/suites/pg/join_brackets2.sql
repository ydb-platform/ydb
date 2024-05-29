--!syntax_pg
SELECT DISTINCT att.attname as name, att.attnum as OID, pg_catalog.format_type(ty.oid,NULL) AS datatype,
att.attnotnull as not_null, att.atthasdef as has_default_val, des.description, seq.seqtypid
FROM pg_catalog.pg_attribute att
    JOIN pg_catalog.pg_type ty ON ty.oid=atttypid
    JOIN pg_catalog.pg_namespace tn ON tn.oid=ty.typnamespace
    JOIN pg_catalog.pg_class cl ON cl.oid=att.attrelid
    JOIN pg_catalog.pg_namespace na ON na.oid=cl.relnamespace
    LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=ty.typelem
    LEFT OUTER JOIN pg_catalog.pg_attrdef def ON adrelid=att.attrelid AND adnum=att.attnum
    LEFT OUTER JOIN (pg_catalog.pg_depend JOIN pg_catalog.pg_class cs ON classid='pg_class'::regclass AND objid=cs.oid AND cs.relkind='S') ON refobjid=att.attrelid AND refobjsubid::int4=att.attnum
    LEFT OUTER JOIN pg_catalog.pg_namespace ns ON ns.oid=cs.relnamespace
    LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid=att.attrelid AND indisprimary
    LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=att.attrelid AND des.objsubid::int4=att.attnum AND des.classoid='pg_class'::regclass)
    LEFT OUTER JOIN pg_catalog.pg_sequence seq ON cs.oid=seq.seqrelid
WHERE

    att.attrelid = 12302::oid
    AND att.attnum > 0
    AND att.attisdropped IS FALSE
ORDER BY att.attnum
