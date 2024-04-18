#!/bin/bash

set -ex

/ydb -p ${PROFILE} yql -s '
    CREATE TABLE simple_table (number Int32, PRIMARY KEY (number));
    COMMIT;
    INSERT INTO simple_table (number) VALUES
      (1),
      (2),
      (3);
    COMMIT;

    CREATE TABLE join_table (id Int32, data STRING, PRIMARY KEY (id));
    COMMIT;
    INSERT INTO join_table (id, data) VALUES
      (1, "ydb10"),
      (2, "ydb20"),
      (3, "ydb30");
    COMMIT;
  '

retVal=$?
if [ $retVal -ne 0 ]; then
  echo $retVal
  exit $retVal
fi

echo $(date +"%T.%6N") "SUCCESS"
