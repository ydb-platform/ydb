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
    CREATE TABLE users (age Int32, id Int32, ip STRING, name STRING, region Int32, PRIMARY KEY(id));
    COMMIT;
    INSERT INTO users (age, id, ip, name, region) VALUES
      (15, 1, "95.106.17.32", "Anya", 213),
      (25, 2, "88.78.248.151", "Petr", 225),
      (17, 3, "93.94.183.63", "Masha", 1),
      (5, 4, "::ffff:193.34.173.188", "Alena", 225),
      (15, 5, "93.170.111.29", "Irina", 2),
      (13, 6, "93.170.111.28", "Inna", 21),
      (33, 7, "::ffff:193.34.173.173", "Ivan", 125),
      (45, 8, "::ffff:133.34.173.188", "Asya", 225),
      (27, 9, "::ffff:133.34.172.188", "German", 125),
      (41, 10, "::ffff:133.34.173.185", "Olya", 225),
      (35, 11, "::ffff:193.34.163.188", "Slava", 2),
      (56, 12, "2a02:1812:1713:4f00:517e:1d79:c88b:704", "Elena", 2),
      (18, 17, "ivalid ip", "newUser", 12);
    COMMIT;
  '

retVal=$?
if [ $retVal -ne 0 ]; then
  echo $retVal
  exit $retVal
fi

echo $(date +"%T.%6N") "SUCCESS"
