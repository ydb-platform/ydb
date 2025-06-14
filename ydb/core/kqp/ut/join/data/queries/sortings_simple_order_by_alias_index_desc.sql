--!syntax_v1
select
    t.id
from `/Root/bank_document` view ix_bank_document_exec_dt_accounts as t
order by t.exec_dt desc, t.id desc
limit 2;
