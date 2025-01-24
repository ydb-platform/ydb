Original regression Postgresql tests are in original/cases
Extracts of original tests used as pre-commit tests are in cases

test.sh runs pre-commit tests. Run ya make in yql/tools/pg-make-test beforehand
original.sh runs original tests

Run make-tests.sh to build pre-commit tests from original tests. pg_tests.csv file is a report file indicating
share of SQL statements per testcase, which run successfully.

Run update-status.sh to update status.md from pg_tests.csv
