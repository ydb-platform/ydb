#!/bin/bash

/ydb -p tests-ydb-client yql -s '
    CREATE TABLE column_selection_A_b_C_d_E_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_A_b_C_d_E_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_COL1_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_COL1_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_asterisk_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_asterisk_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col2_COL1_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col2_COL1_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col2_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col2_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;

    CREATE TABLE column_selection_col3_NATIVE (COL1 Int32, col2 Int32, PRIMARY KEY (COL1));
    COMMIT;
    INSERT INTO column_selection_col3_NATIVE (COL1, col2) VALUES
      (1, 2),
      (10, 20);
    COMMIT;
  '

echo $(date +"%T.%6N") "SUCCESS"
