ARG BASE_IMAGE=ghcr.io/ydb-platform/fq-connector-go:v0.11.0-rc.1@sha256:58a1e12de21ef3b403450b176b6220fd545d2802ce16dbc3b05ec4c91e075cdc
FROM $BASE_IMAGE

COPY ./fq-connector-go.yaml /fq-connector-go.yaml
COPY ./entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

HEALTHCHECK --interval=10s --timeout=300s --retries=30 \
  CMD cat /var/log/log.txt | grep 'server/service_connector.go:182\tstarting GRPC server' || exit 1


