# syntax=docker/dockerfile:1
FROM ubuntu:22.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC


RUN apt-get update \
    && apt-get install -y --no-install-recommends git wget gnupg lsb-release curl xz-utils tzdata \
    cmake python3-dev python3-pip ninja-build antlr3 m4 libidn11-dev libaio1 libaio-dev make \
    clang-12 lld-12 llvm-12 clang-14 lld-14 llvm-14 file \
    && pip3 install conan==1.59 grpcio-tools pyinstaller==5.13.2 six pyyaml packaging PyHamcrest cryptography \
    && (V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
         tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)


COPY ydb/ /ydbwork/ydb/

WORKDIR /ydbwork/

RUN --mount=type=secret,id=ccache_remote_storage \
    mkdir build && cd build \
    && export CONAN_USER_HOME=/ydbwork/build \
    && export CCACHE_BASEDIR=/ydbwork/ \
    && export CCACHE_SLOPPINESS=locale \
    && export CCACHE_REMOTE_STORAGE="$(cat /run/secrets/ccache_remote_storage)" \
    && export CC=/usr/bin/clang-14 \
    && export CC_FOR_BUILD=$CC \
    && cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
        -DCCACHE_PATH=/usr/local/bin/ccache \
        -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain \
        ../ydb \
    && ninja ydb/apps/ydbd/ydbd ydb/apps/ydb/all \
    && ccache -s \
    && strip ydb/apps/ydbd/ydbd \
    && strip ydb/apps/ydb/ydb \
    && mv ./ydb/apps/ydbd/ydbd / \
    && mv ./ydb/apps/ydb/ydb / \
    && /ydbd -V  \
    && /ydb version \
    && cd .. && rm -rf build  # for reduce cache size



# always use local_ydb.spec from main revision
COPY main/ydb/public/tools/local_ydb/local_ydb.spec ydb/ydb/public/tools/local_ydb/

RUN cd ydb && \
    ./ydb/tests/oss/launch/compile_protos.sh . ydb library/cpp/actors && \
    cd ydb/public/tools/local_ydb/ && \
    pyinstaller local_ydb.spec && \
    ./dist/local_ydb --help


FROM ubuntu:22.04
RUN apt-get update  \
    && apt-get install --no-install-recommends -y libidn12 libaio1 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/ydb/bin/ \
    && mkdir -p /ydb_data \
    && mkdir -p ydb_certs \
    && echo '{"check_version":false}' > root/ydb/bin/config.json

COPY main/.github/docker/files/ /
COPY --from=builder /ydbwork/ydb/ydb/public/tools/local_ydb/dist/local_ydb /
COPY --from=builder /ydbd /ydb /


# YDB grpc
EXPOSE ${GRPC_TLS_PORT:-2135}
EXPOSE ${GRPC_PORT:-2136}
EXPOSE ${MON_PORT:-8765}

HEALTHCHECK --start-period=60s --interval=1s CMD sh ./health_check

CMD ["sh", "./initialize_local_ydb"]
