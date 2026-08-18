#!/bin/sh

# echo "$(dig tests-fq-streaming-generic-ydb +short) tests-fq-generic-streaming-ydb" >> /etc/hosts
sed '/\(127.0.0.1\|::1\).*localhost/s/^/# /' /etc/hosts >/tmp/hosts
cat /tmp/hosts > /etc/hosts
cat /etc/hosts

/opt/ydb/bin/fq-connector-go server -c /fq-connector-go.yaml 2>&1 | tee /var/log/log.txt
