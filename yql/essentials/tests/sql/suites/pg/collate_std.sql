--!syntax_pg
select
    upper('straße' COLLATE "default"),
    upper('straße' COLLATE "C"),
    upper('straße' COLLATE "POSIX"),
    upper('straße' COLLATE "ucs_basic"),
    upper('straße' COLLATE "unicode"),
    upper('i' COLLATE "default"),
    upper('i' COLLATE "C"),
    upper('i' COLLATE "POSIX"),
    upper('i' COLLATE "ucs_basic"),
    upper('i' COLLATE "unicode");
