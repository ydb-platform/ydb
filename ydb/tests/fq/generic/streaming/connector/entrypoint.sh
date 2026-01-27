#!/bin/sh

echo "$(dig tests-fq-generic-streaming-ydb +short) tests-fq-generic-streaming-ydb" >> /etc/hosts
cat /etc/hosts

/opt/ydb/bin/fq-connector-go server -c /fq-connector-go.yaml 2>&1 | tee /var/log/log.txt
