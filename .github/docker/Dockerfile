# syntax=docker/dockerfile:1
FROM ubuntu:22.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      python3 curl lsb-release python3-dev libc6-dev libidn11-dev libaio1 libaio-dev libgcc-11-dev binutils tree git

COPY ydbd/ /ydbwork/ydbd/
WORKDIR /ydbwork/ydbd/
RUN ./ya make -r -T \
      -C ydb/apps/ydbd/ydbd \
      -o /ydbwork/out/ \
    && /ydbwork/out/ydb/apps/ydbd/ydbd -V \
    && tree /ydbwork/out/ \
    && strip /ydbwork/out/ydb/apps/ydbd/ydbd \
    && rm -rf ~/.ya  || echo 'unable to delete ~/.ya' # for reduce cache size

COPY cli/ /ydbwork/cli/
WORKDIR /ydbwork/cli/
RUN ./ya make -r -T \
      -C ydb/apps/ydb/ydb \
      -o /ydbwork/out/ \
    && /ydbwork/out/cli/apps/ydb/ydb version \
    && tree /ydbwork/out/ \
    && strip /ydbwork/out/ydb/apps/ydb/ydb \
    && rm -rf ~/.ya  || echo 'unable to delete ~/.ya' # for reduce cache size

COPY local_ydb/ /ydbwork/local_ydb/
WORKDIR /ydbwork/local_ydb/
RUN ./ya make -r -T \
      -C ydb/public/tools/local_ydb/local_ydb \
      -o /ydbwork/out/ \
    && tree /ydbwork/out/ \
    && strip /ydbwork/out/ydb/public/tools/local_ydb/local_ydb \
    && rm -rf ~/.ya  || echo 'unable to delete ~/.ya' # for reduce cache size

FROM ubuntu:22.04
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 jq \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /ydb_data && mkdir -p /ydb_certs

COPY main/.github/docker/files/ /
RUN chmod +x /health_check /initialize_local_ydb /health_liveness /health_readiness
COPY --from=builder \
    /ydbwork/out/ydb/apps/ydbd/ydbd \
    /ydbwork/out/ydb/apps/ydb/ydb \
    /ydbwork/out/ydb/public/tools/local_ydb/local_ydb \
    /

# YDB grpc
EXPOSE ${GRPC_TLS_PORT:-2135}
EXPOSE ${GRPC_PORT:-2136}
EXPOSE ${MON_PORT:-8765}
EXPOSE ${YDB_KAFKA_PROXY_PORT:-9092}

# pass envs to container runtime
ENV GRPC_PORT=${GRPC_PORT:-2136}
ENV GRPC_TLS_PORT=${GRPC_TLS_PORT:-2135}
ENV MON_PORT=${MON_PORT:-8765}
ENV YDB_KAFKA_PROXY_PORT=${YDB_KAFKA_PROXY_PORT:-9092}

HEALTHCHECK --start-period=5s --interval=2s --timeout=3s --retries=3 CMD bash /health_check

ENTRYPOINT ["bash", "/initialize_local_ydb"]
