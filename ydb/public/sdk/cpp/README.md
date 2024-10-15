# YDB C++ SDK
## This is a pre-release version of the C++ driver for [YDB](https://github.com/ydb-platform/ydb), backward compatibility is not guaranteed!

## Building YDB C++ SDK from sources

### Prerequisites

- cmake 3.22+
- clang 16+
- git 2.20+
- ninja 1.10+
- ragel
- yasm
- protoc

### Library dependencies

- gRPC
- protobuf
- OpenSSL
- Iconv
- IDN
- rapidjson
- xxhash
- zlib
- zstd
- lz4
- snappy 1.1.8+
- base64
- brotli 1.1.0+
- double-conversion
- jwt-cpp

### Runtime requirements

- libidn11-dev (IDN)
- libiconv (Iconv)

### Testing requirements

- gtest
- gmock

### Install dependencies

```bash
sudo apt-get -y update
sudo apt-get -y install git cmake ninja-build libidn11-dev ragel yasm protobuf-compiler \
  protobuf-compiler-grpc libprotobuf-dev libgrpc++-dev libgrpc-dev libgrpc++1 libgrpc10 \
  rapidjson-dev zlib1g-dev libxxhash-dev libzstd-dev libsnappy-dev liblz4-dev \
  libgtest-dev libgmock-dev libbz2-dev libdouble-conversion-dev libssl-dev

wget https://apt.llvm.org/llvm.sh
chmod u+x llvm.sh
sudo ./llvm.sh 16

wget https://ftp.gnu.org/pub/gnu/libiconv/libiconv-1.15.tar.gz
tar -xvzf libiconv-1.15.tar.gz && cd libiconv-1.15
./configure --prefix=/usr/local
make
sudo make install

wget -O base64-0.5.2.tar.gz https://github.com/aklomp/base64/archive/refs/tags/v0.5.2.tar.gz
tar -xvzf base64-0.5.2.tar.gz && cd base64-0.5.2
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
sudo cmake --build . --config Release --target install

wget -O brotli-1.1.0.tar.gz https://github.com/google/brotli/archive/refs/tags/v1.1.0.tar.gz
tar -xvzf brotli-1.1.0.tar.gz && cd brotli-1.1.0
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
sudo cmake --build . --config Release --target install

wget -O jwt-cpp-0.7.0.tar.gz https://github.com/Thalhammer/jwt-cpp/archive/refs/tags/v0.7.0.tar.gz
tar -xvzf jwt-cpp-0.7.0.tar.gz && cd jwt-cpp-0.7.0
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
sudo cmake --build . --config Release --target install
```

### Create the work directory

```bash
mkdir ~/ydbwork && cd ~/ydbwork
mkdir build
```

### Install ccache

Install `ccache` into `/usr/local/bin/`. The recommended version is `4.8.1` or above, the minimum required version is `4.7`.

```bash
(V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
sudo tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)
```

### Clone the ydb-cpp-sdk repository

```bash
git clone --recurse-submodules https://github.com/ydb-platform/ydb-cpp-sdk.git
```

### Configure

Generate build configuration using the `release` preset. `ccache` is located automatically, but if you get the warning that it's not been found, specify its location by passing `-DCCACHE_PATH=path/to/bin`

```bash
cd ydb-cpp-sdk
cmake --preset release
```

### Build

```bash
cmake --build --preset release
```

### Test

Specify a level of parallelism by passing the `-j<level>` option into the command below (e.g. `-j$(nproc)`)

Running all tests:

```bash
ctest -j$(nproc) --preset release
```

Running unit tests only:

```bash
ctest -j$(nproc) --preset release-unit
```

Running integration tests only:

```bash
ctest -j$(nproc) --preset release-integration
```

Note that some tests use a legacy test library instead of GoogleTest, see `./<test_target> --help` for details. If you need to run only certain test cases, here is an alternative for `--gtest_filter` option:

```bash
cat <<EOF | ./<test_target> --filter-file /dev/fd/0
-ExcludedTestCase
+IncludedTestCase
+IncludedTestCase::TestName
EOF
```
