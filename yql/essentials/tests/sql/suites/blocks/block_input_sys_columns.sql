USE plato;

PRAGMA yt.JobBlockInput;

$a = (
    SELECT TableName() AS table_name, TableRecordIndex() AS record_index, Input1.* FROM Input1
    UNION ALL
    SELECT TableName() AS table_name, TableRecordIndex() AS record_index, Input2.* FROM Input2
);

$b = (
    SELECT * FROM Input1 AS users
    UNION ALL
    SELECT * FROM Input2 AS users
);

SELECT * FROM $a ORDER BY table_name, record_index;
SELECT ROW_NUMBER() OVER () AS row_num, b.* FROM $b AS b ORDER BY row_num;
