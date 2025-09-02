#!/usr/bin/env bash
set -eux
cp ../v0_proto/ya.make.gen ../v0_proto/ya.make
yag make ../v0_proto --add-result=".h" --add-result=".cc"
rm ../v0_proto/ya.make
python3 ../multiproto.py SQLParser ../v0_proto .

