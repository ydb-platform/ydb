#!/bin/bash


while read r; do
ya ydb -e grpcs://lb.etnou58eeskoehetr1ta.ydb.mdb.yandexcloud.net:2135  -d /global-yaem/yc.yaem.service-cloud/etnou58eeskoehetr1ta sql -s "$r" </dev/null &
sleep 5s
done < 1.sql 
wait
echo Done
