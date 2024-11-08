# syntax=docker/dockerfile:1.4
ARG BREAKPAD_IMAGE_TAG=v2022.07.12
###
# Base image with required deb packages
###
FROM cr.yandex/mirror/ubuntu:focal AS base
RUN \
    apt-get -yqq update && \
    apt-get -yqq install libcap2-bin ca-certificates && \
    apt-get -yqq clean all && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r ydb && \
    useradd --no-log-init -r -m -g ydb -G disk ydb

FROM base AS base-debug
RUN \
    apt-get -yqq update && \
    apt-get -yqq install dnsutils telnet netcat-openbsd iputils-ping gdb atop strace curl linux-tools-generic && \
    apt-get -yqq clean all && \
    rm -rf /var/lib/apt/lists/*

FROM scratch AS license
# release information
COPY --chmod=0644 /AUTHORS /AUTHORS
COPY --chmod=0644 /LICENSE /LICENSE
COPY --chmod=0644 /README.md /README.md

FROM scratch AS libs
# dynamic libraries
COPY --chmod=0644 /libiconv.so /lib/libiconv.so
COPY --chmod=0644 /liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --chmod=0644 /liblibaio-dynamic.so /lib/liblibaio-dynamic.so

###
# Image with setcap'ed ydb binary
###
FROM base AS ydbd-setcap
COPY --chmod=0755 --chown=ydb /ydbd /ydbd
# workaround for decrease image size
RUN /sbin/setcap CAP_SYS_RAWIO=ep /ydbd

###
# Release image
###
FROM base AS release
# release information
COPY --link --from=license /AUTHORS /AUTHORS
COPY --link --from=license /LICENSE /LICENSE
COPY --link --from=license /README.md /README.md
# dynamic libraries
COPY --link --from=libs /lib/libiconv.so /lib/libiconv.so
COPY --link --from=libs /lib/liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --link --from=libs /lib/liblibaio-dynamic.so /lib/liblibaio-dynamic.so
# ydb binaries
COPY --chmod=0755 --chown=ydb /ydb /opt/ydb/bin/ydb
COPY --link --from=ydbd-setcap /ydbd /opt/ydb/bin/ydbd

WORKDIR /opt/ydb/bin
USER ydb

###
# Breakpad image
###
FROM cr.yandex/crp2lrlsrs36odlvd8dv/breakpad_init:$BREAKPAD_IMAGE_TAG AS breakpad

###
# Debug image with additional packages
###
FROM base-debug AS debug
# release information
COPY --link --from=license /AUTHORS /AUTHORS
COPY --link --from=license /LICENSE /LICENSE
COPY --link --from=license /README.md /README.md
# dynamic libraries
COPY --link --from=libs /lib/libiconv.so /lib/libiconv.so
COPY --link --from=libs /lib/liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --link --from=libs /lib/liblibaio-dynamic.so /lib/liblibaio-dynamic.so
# ydb binaries
COPY --chmod=0755 --chown=ydb /ydb /opt/ydb/bin/ydb
COPY --link --from=ydbd-setcap /ydbd /opt/ydb/bin/ydbd

ENV BREAKPAD_MINIDUMPS_PATH=/opt/ydb/volumes/coredumps/
ENV BREAKPAD_MINIDUMPS_SCRIPT=/opt/ydb/bin/minidump_script.py
ENV LD_PRELOAD=libbreakpad_init.so
COPY --chmod=4644 --link --from=breakpad /usr/lib/libbreakpad_init.so /usr/lib/libbreakpad_init.so
COPY --chmod=0755 --link --from=breakpad /usr/bin/minidump-2-core /usr/bin/minidump-2-core
COPY --chmod=0755 --chown=ydb /minidump_script.py /opt/ydb/bin/minidump_script.py
COPY --chmod=0644 --chown=ydb /ydbd.debug /opt/ydb/bin/ydbd.debug
