SELECT
    doc.document_id AS document_id
FROM (
SELECT d.id AS document_id, d.exec_dt AS exec_dt
FROM `/Root/bank_document` VIEW ix_bank_document_exec_dt_accounts AS d
WHERE d.exec_dt >= Cast('1990-12-10' as Date)
ORDER BY exec_dt, document_id
LIMIT 1000
) AS doc;