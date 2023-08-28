# syntax=docker/dockerfile:1
FROM cr.yandex/yc/yandex-docker-local-ydb@sha256:ce95513262a31fbe5b309521c362bde6d24ea68b7957bd874ef43d985a5d5e45 AS yandex_local_build
FROM ubuntu:22.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC


RUN apt-get update \
    && apt-get install -y sudo wget gnupg lsb-release curl xz-utils tzdata \
    cmake python3-pip ninja-build antlr3 m4 libidn11-dev libaio1 libaio-dev make \
    clang-12 lld-12 llvm-12 clang-14 lld-14 llvm-14 \
    && pip3 install conan==1.59 grpcio-tools \
    && (V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
         tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)


RUN apt-get install -y --no-install-recommends git

WORKDIR /ydbwork

ARG GIT_REPO=https://github.com/ydb-platform/ydb.git
ARG GIT_REF=main

ENV GIT_REPO=$GIT_REPO
ENV GIT_REF=$GIT_REF

RUN mkdir ydb \
    && git clone --depth=1 -b "${GIT_REF?}" ${GIT_REPO?}

RUN --mount=type=secret,id=ccache_remote_storage \
    mkdir build && cd build \
    && export CONAN_USER_HOME=/ydbwork/build \
    && export CCACHE_BASEDIR=/ydbwork/ \
    && export CCACHE_SLOPPINESS=locale \
    && export CCACHE_REMOTE_STORAGE="$(cat /run/secrets/ccache_remote_storage)" \
    && cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER_LAUNCHER=/usr/local/bin/ccache -DCMAKE_CXX_COMPILER_LAUNCHER=/usr/local/bin/ccache \
        -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain \
        ../ydb \
    && ninja ydb/apps/ydbd/ydbd ydb/apps/ydb/all \
    && ccache -s \
    && strip ydb/apps/ydbd/ydbd \
    && strip ydb/apps/ydb/ydb

FROM ubuntu:22.04
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/ydb/bin/ \
    && mkdir -p /ydb_data \
    && mkdir -p ydb_certs \
    && echo '{"check_version":false}' > root/ydb/bin/config.json

COPY --from=builder /ydbwork/build/ydb/apps/ydbd/ydbd /ydbwork/build/ydb/apps/ydb/ydb /

COPY --from=yandex_local_build /local_ydb /

COPY files/initialize_local_ydb.sh /initialize_local_ydb
COPY files/health_check.sh /health_check
COPY files/THIRD_PARTY_LICENSES /THIRD_PARTY_LICENSES
COPY files/LICENSE /LICENSE


RUN /ydbd -V
RUN /ydb version

# YDB grpc
EXPOSE ${GRPC_TLS_PORT:-2135}
EXPOSE ${GRPC_PORT:-2136}
EXPOSE ${MON_PORT:-8765}

HEALTHCHECK --start-period=60s --interval=1s CMD sh ./health_check

CMD ["sh", "./initialize_local_ydb"]
