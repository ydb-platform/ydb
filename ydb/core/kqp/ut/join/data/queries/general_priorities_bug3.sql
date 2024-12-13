PRAGMA ydb.EnableOrderPreservingLookupJoin="true";
PRAGMA ydb.CostBasedOptimizationLevel="1";

SELECT
    doc.id AS document_id
FROM (
SELECT d.id AS id, d.exec_dt AS exec_dt
FROM `/Root/bank_document` VIEW ix_bank_document_exec_dt_accounts AS d
LEFT JOIN `/Root/bank_sub_document` AS sd
  ON d.id = sd.document_id
WHERE sd.document_id IS NULL
  AND d.exec_dt >= Cast('1990-12-10' as Date)
  AND d.acc_dt_id = 15
ORDER BY exec_dt, id
LIMIT 1000
) AS doc;