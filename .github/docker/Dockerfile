# syntax=docker/dockerfile:1
FROM ubuntu:22.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      python3 curl lsb-release python3-dev libc6-dev libidn11-dev libaio1 libaio-dev libgcc-11-dev binutils tree git

COPY ydb/ /ydbwork/ydb/

WORKDIR /ydbwork/ydb/

RUN ./ya make -r -T \
      -C ydb/apps/ydbd/ydbd \
      -C ydb/apps/ydb/ydb \
      -C ydb/public/tools/local_ydb/local_ydb \
      -o /ydbwork/out/ \
    && /ydbwork/out/ydb/apps/ydbd/ydbd -V \
    && tree /ydbwork/out/ \
    && strip /ydbwork/out/ydb/apps/ydbd/ydbd /ydbwork/out/ydb/apps/ydb/ydb /ydbwork/out/ydb/public/tools/local_ydb/local_ydb \
    && rm -rf ~/.ya  || echo 'unable to delete ~/.ya' # for reduce cache size


FROM ubuntu:22.04
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/ydb/bin/ \
    && mkdir -p /ydb_data \
    && mkdir -p ydb_certs \
    && echo '{"check_version":false}' > root/ydb/bin/config.json

COPY main/.github/docker/files/ /
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

HEALTHCHECK --start-period=60s --interval=1s CMD sh /health_check

CMD ["sh", "/initialize_local_ydb"]
