ARG BASE_IMAGE=ghcr.io/ydb-platform/fq-connector-go:v0.7.2-rc.4@sha256:3dd675ebdba9ff4a1933443d801ba121d11a9eec20a6da8f0fa5dddc4ba97c0f
FROM $BASE_IMAGE

COPY ./fq-connector-go.yaml /fq-connector-go.yaml
COPY ./entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

HEALTHCHECK --interval=10s --timeout=300s --retries=30 --start-period=10s\
  CMD cat /var/log/log.txt | grep 'server/service_connector.go:182\tstarting GRPC server' || exit 1

