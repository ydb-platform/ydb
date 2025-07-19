PRAGMA ydb.EnableOrderPreservingLookupJoin="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

select *
    from `/Root/bank_sub_document` sd
inner join
    `/Root/bank_document` d
on d.id = sd.document_id
where
    blah2 = 'dota2pudge' AND
    kostya = 1337 AND
    vedernikoff = document_id AND
    blah_blah = blah2
order by blah_blah desc, blah2, vedernikoff, kostya desc, blah
limit 1000;
