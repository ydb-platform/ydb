PRAGMA ydb.EnableOrderPreservingLookupJoin="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

SELECT d.id AS id, d.exec_dt AS exec_dt
FROM `/Root/bank_document` VIEW ix_bank_document_exec_dt_accounts AS d
INNER JOIN `/Root/bank_sub_document` VIEW IX_BANK_SUB_DOCUMENT_DOCUMENT_ID AS sd
    ON d.id = sd.document_id
WHERE
    d.id = d.kostya AND
    acc_dt_id = 1337
ORDER BY exec_dt, acc_dt_id, d.kostya
LIMIT 1000;
