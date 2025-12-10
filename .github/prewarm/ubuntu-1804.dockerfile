# syntax=docker/dockerfile:1.0
FROM ubuntu:18.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

## prepare system
RUN apt-get update \
    && apt-get install -y wget gnupg lsb-release curl xz-utils tzdata software-properties-common \
    && wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | apt-key add - \
    && echo "deb http://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/kitware.list \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-14 main" > /etc/apt/sources.list.d/llvm.list \
    && add-apt-repository ppa:ubuntu-toolchain-r/test \
    && apt-get update


RUN apt-get install -y --no-install-recommends python3.8 python3.8-venv python3-venv \
    && python3.8 -m venv /opt/ve && /opt/ve/bin/pip install -U pip

ENV PATH=/opt/ve/bin:$PATH

RUN apt-get install -y --no-install-recommends git cmake ninja-build antlr3 m4 clang-14 lld-14 libidn11-dev libaio1 libaio-dev llvm-14 make \
    && pip install conan==1.59 grpcio-tools==1.57.0 \
    && (V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
     tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)
