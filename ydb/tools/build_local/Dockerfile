FROM ubuntu:22.04@sha256:5ec03bb3441e8b0bf3b4f9cd4629a1ae763010dc3035bb8da3ae6cf026486401

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates curl git python3 python3-dev \
        libc6-dev libidn11-dev libaio1 libaio-dev libgcc-11-dev binutils \
    && rm -rf /var/lib/apt/lists/*

ARG BUILDER_UID=1000
ARG BUILDER_GID=1000
RUN groupadd --non-unique --gid "${BUILDER_GID}" builder \
    && useradd --non-unique --uid "${BUILDER_UID}" --gid builder --create-home --shell /bin/bash builder \
    && install -d -o builder -g builder /home/builder/.ya /out

COPY entrypoint.sh /usr/local/bin/ydb-build
RUN chmod 755 /usr/local/bin/ydb-build

USER builder
ENV YA_CACHE_DIR=/home/builder/.ya
WORKDIR /src
ENTRYPOINT ["/usr/local/bin/ydb-build"]
