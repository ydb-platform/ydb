#! /bin/sh
../../tools/pg-make-test/pg-make-test \
  --srcdir=original/cases \
  --patchdir=patches \
  --dstdir=cases \
  --runner=../../tools/pgrun/pgrun \
  --splitter="../../tools/pgrun/pgrun split-statements" \
  --skip=uuid,char,varchar \
  --report=pg_tests.csv \
  $@

