# pg-make-test

This tool extracts a subset of SQL statements which work as expected
from Postgres' regression tests located in ydb/library/yql/tests/postgresql/original/cases.
The extracted tests are normally saved into ydb/library/yql/tests/postgresql/cases.
The tests could optionally get patched.

For each .sql test file the tool expects a matching .out file(s) in the
same dir. For each pair of .sql/.out files the working subset of SQL
statements is written to the output dir.
