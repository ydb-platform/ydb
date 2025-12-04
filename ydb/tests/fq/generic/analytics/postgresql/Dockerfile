ARG BASE_IMAGE=postgres:15-bullseye@sha256:3411b9f2e5239cd7867f34fcf22fe964230f7d447a71d63c283e3593d3f84085
FROM $BASE_IMAGE

COPY ./init.sql /docker-entrypoint-initdb.d/init.sql

HEALTHCHECK --interval=10s --timeout=50s --retries=5 --start-period=30s \
  CMD pg_isready -q -h localhost -p 6432 -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-db} || exit 1


