REPLACE INTO `/Root/bank_document` (id, exec_dt, acc_dt_id) VALUES
    (99, Unwrap(Cast('1991-12-10' as Date)), 15),
    (100, Unwrap(Cast('1991-12-10' as Date)), 15),
    (101, Unwrap(Cast('1991-12-10' as Date)), 15),
    (102, Unwrap(Cast('1991-12-10' as Date)), 15),
    (103, Unwrap(Cast('1991-12-10' as Date)), 15);

REPLACE INTO `/Root/bank_sub_document` (document_id, blah, blah_blah, blah2) VALUES
    (101, "blah", "blah_blah", "blah2"),
    (102, "blah2", "blah_blah", "blah2");
