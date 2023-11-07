#!/usr/bin/env bash
# Run on clean reposiory (changes should be commited)

set -eEu
trap 'echo Script failed!' ERR 

rm -rf postgresql.patched
cp -a postgresql postgresql.patched
./copy_src.sh > /dev/null 2>&1
diff -ruN postgresql postgresql.patched || true
rm -rf postgresql.patched
