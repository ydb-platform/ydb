# syntax=docker/dockerfile:1.4
FROM cr.yandex/mirror/ubuntu:focal AS breakpad-base
RUN \
    apt-get -yqq update && \
    apt-get -yqq install git build-essential libz-dev python3 curl && \
    apt-get -yqq clean all && \
    rm -rf /var/lib/apt/lists/*
RUN git clone https://chromium.googlesource.com/chromium/tools/depot_tools.git
ENV PATH="/depot_tools:${PATH}"

ARG BREAKPAD_GIT_TAG="v2022.07.12"
FROM breakpad-base AS breakpad-build
COPY --link breakpad_init.cc /breakpad/breakpad_init.cc
RUN \
    cd breakpad && \
    fetch breakpad && \
    cd src && \
    git checkout -- . && git checkout tags/${BREAKPAD_GIT_TAG} && \
    ./configure && make && \
    g++ -std=c++11 -shared -Wall -o ../libbreakpad_init.so -fPIC ../breakpad_init.cc -Isrc/ -Lsrc/client/linux/ -lbreakpad_client -lpthread

FROM scratch AS breakpad-release

COPY --link --from=breakpad-build /breakpad/libbreakpad_init.so /usr/lib/libbreakpad_init.so
COPY --link --from=breakpad-build /breakpad/src/src/tools/linux/md2core/minidump-2-core /usr/bin/minidump-2-core
COPY --link --from=breakpad-build /breakpad/src/src/processor/minidump_stackwalk /usr/bin/minidump_stackwalk
