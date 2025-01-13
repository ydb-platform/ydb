PRAGMA warning('disable', '4510');

SELECT
    StartsWith('test1'u, 'tes'p),
    EndsWith('test1'u, 't1'p),
    YQL::StringContains('test1'u, 'est'p)
;

SELECT
    StartsWith('test1'u, 'tes'pb),
    EndsWith('test1'u, 't1'pb),
    YQL::StringContains('test1'u, 'est'pb)
;

SELECT
    StartsWith('test1', 'tes'p),
    EndsWith('test1', 't1'p),
    YQL::StringContains('test1', 'est'p)
;

SELECT
    StartsWith('test1', 'tes'pv),
    EndsWith('test1', 't1'pv),
    YQL::StringContains('test1', 'est'pv)
;

SELECT
    StartsWith('test1'u, 'x'pv),
    EndsWith(Nothing(Utf8?), 'x'pv),
    YQL::StringContains('test1'u, PgCast(NULL, PgVarChar))
;
