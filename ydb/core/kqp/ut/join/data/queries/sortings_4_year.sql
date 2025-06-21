PRAGMA ydb.EnableOrderPreservingLookupJoin="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

select *
    from `/Root/bank_document`
where kostya = 228 and id < 1337
order by kostya, id desc
limit 1000;
