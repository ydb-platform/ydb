# syntax=docker/dockerfile:1.4
ARG BASE_IMAGE="cr.yandex/mirror/ubuntu"
ARG BASE_IMAGE_TAG="focal"
FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG} AS breakpad-base
RUN \
    apt-get -yqq update && \
    apt-get -yqq install --no-install-recommends ca-certificates git build-essential libz-dev python3 curl && \
    apt-get clean all && rm -rf /var/lib/apt/lists/*
RUN git clone https://chromium.googlesource.com/chromium/tools/depot_tools.git
ENV PATH="/depot_tools:${PATH}"

FROM breakpad-base AS breakpad-build
ARG BREAKPAD_GIT_TAG="v2023.06.01"
RUN \
    mkdir breakpad && cd breakpad && \
    fetch breakpad && cd src && \
    git checkout -- . && git checkout tags/${BREAKPAD_GIT_TAG} && \
    ./configure && make
WORKDIR /breakpad/src
COPY /breakpad_init.cc /breakpad/breakpad_init.cc
RUN g++ -std=c++11 -shared -Wall -o ../libbreakpad_init.so -fPIC ../breakpad_init.cc -Isrc/ -Lsrc/client/linux/ -lbreakpad_client -lpthread

FROM scratch AS breakpad-release
COPY --link --from=breakpad-build /breakpad/libbreakpad_init.so /usr/lib/libbreakpad_init.so
COPY --link --from=breakpad-build /breakpad/src/src/tools/linux/md2core/minidump-2-core /usr/bin/minidump-2-core
COPY --link --from=breakpad-build /breakpad/src/src/processor/minidump_stackwalk /usr/bin/minidump_stackwalk
