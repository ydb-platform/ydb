#!/bin/sh

echo "$(dig tests-fq-generic-analytics-ydb +short) tests-fq-generic-analytics-ydb" >> /etc/hosts
cat /etc/hosts;

/opt/ydb/bin/fq-connector-go server -c /fq-connector-go.yaml 2>&1 | tee /var/log/log.txt
