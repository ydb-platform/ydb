PRAGMA ydb.EnableOrderPreservingLookupJoin="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

SELECT d.id AS id, d.exec_dt AS exec_dt
    FROM `/Root/bank_document` d
ORDER BY d.id
LIMIT 1000;