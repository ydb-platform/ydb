# syntax=docker/dockerfile:1.4
ARG BASE_IMAGE="cr.yandex/mirror/ubuntu"
ARG BASE_IMAGE_TAG="focal"
ARG BREAKPAD_IMAGE="cr.yandex/crp2lrlsrs36odlvd8dv/breakpad_init"
ARG BREAKPAD_IMAGE_TAG="v2022.07.12"

###
# Base image with required deb packages
###
FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG} AS base
RUN groupadd -r ydb && useradd --no-log-init -r -m -g ydb -G disk ydb && \
    apt-get -yqq update && \
    apt-get -yqq install --no-install-recommends libcap2-bin ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

###
# Base image with google brekpad assets
###
FROM ${BREAKPAD_IMAGE}:${BREAKPAD_IMAGE_TAG} AS breakpad_init
FROM base AS base-breakpad
RUN \
    apt-get -yqq update && \
    apt-get -yqq install --no-install-recommends binutils gdb strace linux-tools-generic \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV LD_PRELOAD=libbreakpad_init.so
ENV BREAKPAD_MINIDUMPS_PATH=/opt/ydb/volumes/coredumps
ENV BREAKPAD_MINIDUMPS_SCRIPT=/opt/ydb/bin/minidump_script.py
# breakpad binaries
COPY --chmod=4644 --from=breakpad_init /usr/lib/libbreakpad_init.so /usr/lib/libbreakpad_init.so
COPY --chmod=0755 --from=breakpad_init /usr/bin/minidump_stackwalk /usr/bin/minidump_stackwalk
COPY --chmod=0755 --from=breakpad_init /usr/bin/minidump-2-core /usr/bin/minidump-2-core
# minidump callback script
COPY --chmod=0755 --chown=ydb /minidump_script.py /opt/ydb/bin/minidump_script.py

###
# Base image with debug packages
###
FROM base-breakpad AS base-debug
RUN \
    apt-get -yqq update && \
    apt-get -yqq  --no-install-recommends dnsutils telnet netcat-openbsd iputils-ping curl && \
    apt-get clean all && rm -rf /var/lib/apt/lists/*

FROM scratch AS license
# release information
COPY --chmod=0644 /AUTHORS /AUTHORS
COPY --chmod=0644 /LICENSE /LICENSE
COPY --chmod=0644 /README.md /README.md

FROM scratch AS dynamic-libs
# dynamic libraries
COPY --chmod=0644 /libiconv.so /lib/libiconv.so
COPY --chmod=0644 /liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --chmod=0644 /liblibaio-dynamic.so /lib/liblibaio-dynamic.so

FROM base AS ydb-binary
COPY --chmod=0755 --chown=ydb /ydb /opt/ydb/bin/ydb

FROM base AS ydbd-setcap
COPY --chmod=0755 --chown=ydb /ydbd /opt/ydb/bin/ydbd
# workaround for decrease image size
RUN /sbin/setcap CAP_SYS_RAWIO=ep /opt/ydb/bin/ydbd

###
# Release image
###
FROM base AS release
# release information
COPY --link --from=license /AUTHORS /AUTHORS
COPY --link --from=license /LICENSE /LICENSE
COPY --link --from=license /README.md /README.md
# dynamic libraries
COPY --link --from=dynamic-libs /lib/libiconv.so /lib/libiconv.so
COPY --link --from=dynamic-libs /lib/liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --link --from=dynamic-libs /lib/liblibaio-dynamic.so /lib/liblibaio-dynamic.so
# ydb binaries
COPY --link --from=ydb-binary /opt/ydb/bin/ydb /opt/ydb/bin/ydb
COPY --link --from=ydbd-setcap /opt/ydb/bin/ydbd /opt/ydb/bin/ydbd

WORKDIR /opt/ydb/bin
USER ydb

###
# Breakpad Image
###
FROM base-breakpad AS breakpad
# release information
COPY --link --from=license /AUTHORS /AUTHORS
COPY --link --from=license /LICENSE /LICENSE
COPY --link --from=license /README.md /README.md
# dynamic libraries
COPY --link --from=dynamic-libs /lib/libiconv.so /lib/libiconv.so
COPY --link --from=dynamic-libs /lib/liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --link --from=dynamic-libs /lib/liblibaio-dynamic.so /lib/liblibaio-dynamic.so
# ydb binaries
COPY --link --from=ydb-binary /opt/ydb/bin/ydb /opt/ydb/bin/ydb
COPY --link --from=ydbd-setcap /opt/ydb/bin/ydbd /opt/ydb/bin/ydbd

WORKDIR /opt/ydb/bin
USER ydb

###
# Debug Image
###
FROM base-debug AS debug
# release information
COPY --link --from=license /AUTHORS /AUTHORS
COPY --link --from=license /LICENSE /LICENSE
COPY --link --from=license /README.md /README.md
# dynamic libraries
COPY --link --from=dynamic-libs /lib/libiconv.so /lib/libiconv.so
COPY --link --from=dynamic-libs /lib/liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --link --from=dynamic-libs /lib/liblibaio-dynamic.so /lib/liblibaio-dynamic.so
# ydb binaries
COPY --link --from=ydb-binary /opt/ydb/bin/ydb /opt/ydb/bin/ydb
COPY --link --from=ydbd-setcap /opt/ydb/bin/ydbd /opt/ydb/bin/ydbd
# debug symbols
COPY --chmod=0644 --chown=ydb /ydbd.debug /opt/ydb/bin/ydbd.debug

WORKDIR /opt/ydb/bin
USER ydb
