ARG BASE_IMAGE=ghcr.io/ydb-platform/fq-connector-go_greenplum:6.25.3-6432@sha256:9e862b05719b289b447562fbce6c003916a764a549f924a4175eecd7e7891a0b
FROM $BASE_IMAGE

COPY ./init_db.sh /init_db.sh

HEALTHCHECK --interval=50s --timeout=10s --retries=5 --start-period=20s \
  CMD /usr/local/gpdb/bin/pg_isready -q -h localhost -p 6432 -U gpadmin || exit 1

