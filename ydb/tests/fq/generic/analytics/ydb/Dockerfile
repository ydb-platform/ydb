ARG BASE_IMAGE=ghcr.io/ydb-platform/local-ydb:latest@sha256:9045e00afec1923dc3277564c7b2f829087c2115f45f18e1d38b80bb89f98be6
FROM $BASE_IMAGE

COPY ./init_ydb /init_ydb
COPY ./01_basic.sh /01_basic.sh

HEALTHCHECK --interval=10s --timeout=300s --retries=30 \
  CMD cat /tmp/start | grep 'ydb is started' || exit 1


