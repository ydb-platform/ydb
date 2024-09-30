#! /usr/bin/env bash

QUERIES_NUM=$1
ROWS_NUM=$2

unlink data/y_pipe
mkfifo data/y_pipe

rm -rf data/query
mkdir -p data/query
rm -rf data/result
mkdir -p data/result

> speed.txt
python3 gen.py \
    --rows-number=$ROWS_NUM \
    --queries-number=$QUERIES_NUM \
    --queries-dir=/home/vokayndzop/ydb/ydb/library/yql/tools/dqrun/data/query \
    | cpipe -vt > data/y_pipe 2> speed.txt &
PYTHON_PID=$!
printf "python pid = %d\n" $PYTHON_PID

> log.txt
./dqrun \
    --verbosity=8 \
    --sql \
    --program=data/query/ \
    --gateways-cfg=examples/gateways.conf \
    --fs-cfg=examples/fs.conf \
    --fq-cfg=examples/fq.conf \
    --udfs-dir=../../udfs/common \
    --emulate-pq=match@data/y_pipe \
    --threads=25 \
    &> log.txt &
DQRUN_PID=$!
printf "dqrun pid = %d\n" $DQRUN_PID

> time.txt
python3 killer.py \
    --output=/home/vokayndzop/ydb/ydb/library/yql/tools/dqrun/data/result \
    --rows-number=$ROWS_NUM \
    --queries-number=$QUERIES_NUM \
    &> time.txt &
KILLER_PID=$!
printf "killer pid = %d\n" $KILLER_PID

> cpu.txt
top -p "$DQRUN_PID" | tee cpu.txt
