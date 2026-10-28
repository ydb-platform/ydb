#!/bin/bash

## Set up environment
/opt/python/cp39-cp39/bin/python -mvenv venv
source venv/bin/activate
python3 -m pip install meson ninja
export CXX=g++

## Install google test from source
git clone https://github.com/google/googletest.git -b v1.14.0
cd googletest
mkdir build
cd build
cmake .. -DBUILD_GMOCK=OFF
make install

## Install Intel SDE
curl -o /tmp/sde.tar.xz https://downloadmirror.intel.com/859732/sde-external-9.58.0-2025-06-16-lin.tar.xz
mkdir /tmp/sde && tar -xvf /tmp/sde.tar.xz -C /tmp/sde/
mv /tmp/sde/* /opt/sde && ln -s /opt/sde/sde /usr/bin/sde

## Build x86-simd-sort
cd /xss
meson setup -Dbuild_tests=true --warnlevel 2 --werror --buildtype release builddir
cd builddir
ninja

## Run tests
sde -tgl -- ./testexe
sde -skl -- ./testexe
