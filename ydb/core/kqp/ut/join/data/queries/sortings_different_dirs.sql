PRAGMA ydb.EnableOrderPreservingStreamLookup="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

select *
    from `/Root/bank_sub_document`
order by document_id desc, blah
limit 1000;
