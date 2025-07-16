SELECT
    test_table_1.key_1 AS key,
    test_table_1.key_2 AS key_2,
    test_table_2.key_3 AS key_3,
    test_table_1.data_1 AS data_1,
    test_table_1.data_2 AS data_2,
    test_table_2.data_3 AS data_3,
    test_table_2.data_4 AS data_4
FROM test_table_1
FULL JOIN test_table_2 ON test_table_1.key_1 = test_table_2.key_4
ORDER BY key, key_2, key_3, data_1, data_2, data_3, data_4 LIMIT 10;