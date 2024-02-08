FROM cr.yandex/mirror/ubuntu:focal

ARG ARC_COMMIT_ID
LABEL arc_commit_id=$ARC_COMMIT_ID

RUN \
  apt-get -yqq update && \
  apt-get -yqq install libcap2-bin dnsutils telnet netcat-openbsd iputils-ping gdb atop strace curl linux-tools-generic && \
  apt-get -yqq clean all && \
  rm -rf /var/lib/apt/lists/* && \
  groupadd -r ydb && \
  useradd --no-log-init -r -m -g ydb -G disk ydb

WORKDIR /opt/ydb/bin

COPY ydb ./ydb

COPY libiconv.so /lib/libiconv.so
COPY liblibidn-dynamic.so /lib/liblibidn-dynamic.so
COPY liblibaio-dynamic.so /lib/liblibaio-dynamic.so

COPY ydbd ./ydbd

RUN /sbin/setcap CAP_SYS_RAWIO=ep /opt/ydb/bin/ydbd

USER ydb
