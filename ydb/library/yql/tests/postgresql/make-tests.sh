#! /bin/sh
../../tools/pg-make-test/pg-make-test \
  --srcdir=original/cases \
  --patchdir=patches \
  --dstdir=cases \
  --runner=../../tools/pgrun/pgrun \
  --splitter="../../tools/pgrun/pgrun split-statements" \
  --skip=uuid \
  --udf=../../udfs/common/set/libset_udf.so \
  --udf=../../udfs/common/re2/libre2_udf.so \
  --report=pg_tests.csv \
  $@

