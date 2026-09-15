#!/bin/sh

/opt/ydb/bin/fq-connector-go server -c /opt/ydb/cfg/fq-connector-go.yaml 2>&1 | tee /var/log/log.txt
