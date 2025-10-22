CREATE TABLE `/Root/bank_document` (
    id Int32 NOT NULL,
    exec_dt Date NOT NULL,
    acc_dt_id Int32 NOT NULL,
    kostya Int32 NOT NULL,
    kek Int32 NOT NULL,
    PRIMARY KEY (id),
    INDEX ix_bank_document_exec_dt_accounts GLOBAL ON (exec_dt, id, kek) COVER ( acc_dt_id )
);

CREATE TABLE `/Root/bank_sub_document` (
    document_id Int32 NOT NULL,
    blah String NOT NULL,
    blah_blah String NOT NULL,
    blah2 String NOT NULL,
    stroka String NOT NULL,
    vedernikoff Int32 NOT NULL,
    PRIMARY KEY (document_id, blah),
    INDEX IX_BANK_SUB_DOCUMENT_DOCUMENT_ID GLOBAL ON (document_id) COVER ( blah2 )
);