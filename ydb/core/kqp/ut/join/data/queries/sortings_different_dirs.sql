PRAGMA ydb.EnableOrderPreservingLookupJoin="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

select *
    from `/Root/bank_sub_document`
order by desc document_id, blah
limit 1000;
