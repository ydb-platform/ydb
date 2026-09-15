# syntax=docker/dockerfile:1.4
ARG BASE_IMAGE="cr.yandex/mirror/ubuntu"
ARG BASE_IMAGE_TAG="focal"

###
# Base image with required deb packages
###
FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG} AS base
RUN groupadd -r ydb && useradd --no-log-init -r -m -g ydb -G disk ydb && \
    apt-get -yqq update && \
    apt-get -yqq install --no-install-recommends libcap2-bin ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
# release information
COPY --chmod=0644 /AUTHORS /AUTHORS
COPY --chmod=0644 /LICENSE /LICENSE
COPY --chmod=0644 /README.md /README.md

FROM base AS release
COPY --chmod=0755 --chown=ydb /ydb-dstool /opt/ydb/bin/ydb-dstool
WORKDIR /opt/ydb/bin
USER ydb
