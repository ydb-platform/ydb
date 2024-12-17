# syntax=docker/dockerfile:1
FROM cr.yandex/mirror/ubuntu:focal as base

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
  apt-get -yqq update && \
  apt-get -yqq install libcap2-bin dnsutils telnet netcat-openbsd iputils-ping gdb atop strace curl linux-tools-generic && \
  apt-get -yqq clean all && \
  rm -rf /var/lib/apt/lists/* && \
  groupadd -r ydb && \
  useradd --no-log-init -r -m -g ydb -G disk ydb

FROM base as ydbd-setcap

COPY --link ydbd /ydbd
RUN /sbin/setcap CAP_SYS_RAWIO=ep /ydbd

FROM base

ARG ARC_COMMIT_ID
LABEL arc_commit_id=$ARC_COMMIT_ID

WORKDIR /opt/ydb/bin
COPY --chmod=0755 --chown=ydb:ydb --link ydb /opt/ydb/bin/ydb
COPY --chmod=0644 --link libiconv.so /lib/libiconv.so
COPY --chmod=0644 --link liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY --chmod=0644 --link liblibaio-dynamic.so /lib/liblibaio-dynamic.so
COPY --chmod=0755 --chown=ydb:ydb --link --from=ydbd-setcap /ydbd /opt/ydb/bin/ydbd
USER ydb
