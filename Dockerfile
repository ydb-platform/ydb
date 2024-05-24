FROM ubuntu:22.04
LABEL name="actor_model"

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update

RUN apt-get install -y \
build-essential \
cmake \
clang-12 \
lld-12 \
git \
python2.7 \
python3-pip \
antlr3 \
libaio-dev \
libidn11-dev \
ninja-build

RUN ln -snf /usr/share/zoneinfo/$CONTAINER_TIMEZONE /etc/localtime && echo $CONTAINER_TIMEZONE > /etc/timezone

RUN apt-get install -y python3.8

RUN pip3 install conan==1.59

RUN mkdir -p /home/ydbwork/build

CMD cd /home/ydbwork/build && cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain ../ydb && ninja tools/actor_model/all && bash
