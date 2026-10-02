--!syntax_pg
/* custom error:FuncCall: conflicting explicit collations: default and C*/
select
    substr('foo' COLLATE "default", 1 COLLATE "C")
