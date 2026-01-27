ARG BASE_IMAGE=mirror.gcr.io/clickhouse/clickhouse-server:24.3.12-alpine@sha256:65e5846a0d9672714f2625502b27846563f6d01ec226304cf851aa49004ffde8
FROM $BASE_IMAGE

COPY ./init_db.sh /docker-entrypoint-initdb.d/init_db.sh

HEALTHCHECK --interval=10s --timeout=10s --retries=3 --start-period=10s\
  CMD clickhouse-client --query "SELECT 1" || exit 1



