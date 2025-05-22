pragma warning("disable", "4510");

select StartsWith('test1'u, 'tes'p),
       EndsWith('test1'u, 't1'p),
       YQL::StringContains('test1'u, 'est'p);

select StartsWith('test1'u, 'tes'pb),
       EndsWith('test1'u, 't1'pb),
       YQL::StringContains('test1'u, 'est'pb);

select StartsWith('test1', 'tes'p),
       EndsWith('test1', 't1'p),
       YQL::StringContains('test1', 'est'p);

select StartsWith('test1', 'tes'pv),
       EndsWith('test1', 't1'pv),
       YQL::StringContains('test1', 'est'pv);

select StartsWith('test1'u, 'x'pv),
       EndsWith(Nothing(Utf8?), 'x'pv),
       YQL::StringContains('test1'u, PgCast(null, PgVarChar));

