#!/usr/bin/env bash
set -eux
cp ../v1_proto/ya.make.gen ../v1_proto/ya.make
yag make ../v1_proto --add-result=".h" --add-result=".cc"
rm ../v1_proto/ya.make
python3 ../multiproto.py SQLv1Parser ../v1_proto .

