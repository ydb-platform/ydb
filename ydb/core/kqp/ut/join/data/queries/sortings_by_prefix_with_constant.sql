PRAGMA ydb.EnableOrderPreservingLookupJoin="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

-- query with complex predicate to check if it detects const correctly
SELECT d.id AS id, d.exec_dt AS exec_dt
FROM `/Root/bank_document` VIEW ix_bank_document_exec_dt_accounts AS d
INNER JOIN `/Root/bank_sub_document` VIEW IX_BANK_SUB_DOCUMENT_DOCUMENT_ID AS sd
    ON d.id = sd.document_id
WHERE
        stroka = 'sskvor_ignorit'
        AND d.exec_dt >= Cast('1990-12-10' as Date)
        AND
            NOT(
                d.id != 23 OR
                acc_dt_id != 228
            )
ORDER BY exec_dt, acc_dt_id, d.id
LIMIT 1000;