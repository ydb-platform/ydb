ARG BASE_IMAGE=clickhouse/clickhouse-server:23-alpine@sha256:b078c1cd294632afa2aeba3530e7ba2e568513da23304354f455a25fab575c06
FROM $BASE_IMAGE

COPY ./init.sql /docker-entrypoint-initdb.d/init.sql

HEALTHCHECK --interval=10s --timeout=10s --retries=3 --start-period=10s\
  CMD clickhouse-client --query "SELECT 1" || exit 1



