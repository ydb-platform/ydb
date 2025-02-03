#!/bin/sh -e

if [ $3 -lt $4 ]; then
    echo Error: SELECT duration $4 is greater total duration $3
    exit 1
fi

if [ $3 -lt $5 ]; then
    echo Error: UPSERT duration $5 is greater total duration $3
    exit 1
fi

if [ $3 -eq $4 ]; then
    echo Error UPSERT duration $5 should differ from SELECT duration $4
    exit 1
fi

if [ $4 -lt $5 ]; then
    first=$4
    second=$5
    first_target=select
else
    first=$5
    second=$4
    first_target=update
fi

make ENDPOINT=$1 TENANT=$2 DURATION=$(($3-$second)) SELECT_DELAY=0 SELECT_DURATION=$(($3-$second)) UPSERT_DELAY=0 UPSERT_DURATION=$(($3-$second)) BULK_UPSERT_THREADS=$6 SELECT_THREADS=$7 UPSERT_THREADS=$8 BULK_UPSERT_ROWS=$9 SELECT_ROWS=$10 UPSERT_ROWS=$11 WINDOW=$12 YDB=$13 NO_INIT=1 $14 -f mixedlog.mk clean.dst
make ENDPOINT=$1 TENANT=$2 DURATION=$3 SELECT_DELAY=$4 SELECT_DURATION=$(($3-$4)) UPSERT_DELAY=$5 UPSERT_DURATION=$(($3-$5)) BULK_UPSERT_THREADS=$6 SELECT_THREADS=$7 UPSERT_THREADS=$8 BULK_UPSERT_ROWS=$9 SELECT_ROWS=$10 UPSERT_ROWS=$11 WINDOW=$12 YDB=$13 $14 -f mixedlog.mk init.dst

make ENDPOINT=$1 TENANT=$2 DURATION=$first SELECT_DELAY=0 SELECT_DURATION=$first UPSERT_DELAY=0 UPSERT_DURATION=$first BULK_UPSERT_THREADS=$6 SELECT_THREADS=$7 UPSERT_THREADS=$8 BULK_UPSERT_ROWS=$9 SELECT_ROWS=$10 UPSERT_ROWS=$11 WINDOW=$12 YDB=$13 NO_INIT=1 $14 -f mixedlog.mk bulk_upsert.dst 2>&1
mv upsert_out upsert1_out
make ENDPOINT=$1 TENANT=$2 DURATION=$(($second-$first)) SELECT_DELAY=0 SELECT_DURATION=$(($second-$first)) UPSERT_DELAY=0 UPSERT_DURATION=$(($second-$first)) BULK_UPSERT_THREADS=$6 SELECT_THREADS=$7 UPSERT_THREADS=$8 BULK_UPSERT_ROWS=$9 SELECT_ROWS=$10 UPSERT_ROWS=$11 WINDOW=$12 YDB=$13 NO_INIT=1 $14 -f mixedlog.mk bulk_upsert.dst $first_target.dst 2>&1 | tee $first_target"_out"
mv upsert_out upsert2_out
make ENDPOINT=$1 TENANT=$2 DURATION=$(($3-$second)) SELECT_DELAY=0 SELECT_DURATION=$(($3-$second)) UPSERT_DELAY=0 UPSERT_DURATION=$(($3-$second)) BULK_UPSERT_THREADS=$6 SELECT_THREADS=$7 UPSERT_THREADS=$8 BULK_UPSERT_ROWS=$9 SELECT_ROWS=$10 UPSERT_ROWS=$11 WINDOW=$12 YDB=$13 NO_INIT=1 $14 -f mixedlog.mk bulk_upsert.dst update.dst select.dst 2>&1 | tee total_out
mv upsert_out upsert3_out

grep -A1 ^Txs upsert1_out | grep -v ^Txs | awk '{printf "Upsert txs/sec: %s\n", $2}'
grep -A1 ^Txs upsert2_out | grep -v ^Txs | awk -vt=$first_target '{printf "%s txs/sec: %s\n", t, $2}'
grep -A1 ^Txs upsert3_out | grep -v ^Txs | awk '{printf "Total txs/sec: %s\n", $2}'
