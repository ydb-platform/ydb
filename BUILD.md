# Building YDB from sources

## Build Requirements
 We have tested YDB builds using Ubuntu 18.04, 20.04 and 22.04. Other Linux distributions are likely to work, but additional effort may be needed. Only x86_64 Linux is currently supported.

 Below is a list of packages that need to be installed before building YDB. 'How to Build' section contains step by step instructions to obtain these packages.

 - cmake 3.22+
 - clang-12
 - lld-12
 - git 2.20+
 - python3.8
 - pip3
 - antlr3
 - libaio-dev
 - libidn11-dev
 - ninja 1.10+

 We run multiple clang instances in parallel to speed up the process by default. Each instance of clang may use up to 1GB of RAM, and linking the binary may use up to 16GB of RAM, please make sure your build host has enough resources.

## Runtime Requirements
 The following packages are required to run ydbd server:

 - libidn11
 - libaio

# How to Build

## (optional) Add CMake and LLVM APT repositories (for Ubuntu 18.04 and 20.04)

## Ubuntu 18.04

For Ubuntu 18.04, you have to add CMake and LLVM APT repositories:

```bash
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | sudo apt-key add -
echo "deb http://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null

wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-12 main" | sudo tee /etc/apt/sources.list.d/llvm.list >/dev/null

sudo apt-get update

```

## Ubuntu 20.04

For Ubuntu 20.04, you have to add CMake APT repository:

```bash
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | sudo apt-key add -
echo "deb http://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null

sudo apt-get update

```


## Install dependencies

```bash
sudo apt-get -y install git cmake python3-pip ninja-build antlr3 m4 clang-12 lld-12 libidn11-dev libaio1 libaio-dev llvm-12
sudo pip3 install conan==1.59

```

## Create the work directory. 
> :warning: Please make sure you have at least 80Gb of free space. We also recommend placing this directory on SSD to reduce build times.

```bash
mkdir ~/ydbwork && cd ~/ydbwork
mkdir build

```

## Clone the ydb repository.

```bash
git clone https://github.com/ydb-platform/ydb.git
```

## Configure



### Configure without Ccache

Run cmake to generate build configuration:

```bash
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS_RELEASE="-O2 -UNDEBUG" -DCMAKE_CXX_FLAGS_RELEASE="-O2 -UNDEBUG" -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain ../ydb

```


### (optional) Configure with Ccache

With enabled Ccache, you can finish the compilation of all targets on supported Linux distributions in a few minutes. 
  Or just `ydbd` or `ydb` binary in a couple of seconds. To optionally enable `Ccache` and enhance the compilation speed, follow these steps:

1. Install `Ccache` into `/usr/local/bin/` (We are using version `4.8.1`, you can use any version greater than `4.7`)
    ```bash
    (V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
     sudo tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)

    ```

2. Configure `Ccache` to use remote storage using environment variables
    ```bash
    export CCACHE_REMOTE_STORAGE="http://cachesrv.ydb.tech:8080|read-only|layout=bazel"
    export CCACHE_SLOPPINESS=locale
    export CCACHE_BASEDIR=~/ydbwork/
   
    ```
    <details>
    <summary>or using Ccache config file</summary>

    ```bash
    ccache -o remote_storage="http://cachesrv.ydb.tech:8080|read-only|layout=bazel"
    ccache -o sloppiness=locale 
    ccache -o base_dir=~/ydbwork/
   
    ```
    </details>
3. Also, you should change Conan's home folder to the build folder for better cache hit 
    ```bash
    export CONAN_USER_HOME=~/ydbwork/build
    ```

4. Genreate build configuration using `ccache`
    ```bash
    cd build
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER_LAUNCHER=/usr/local/bin/ccache -DCMAKE_CXX_COMPILER_LAUNCHER=/usr/local/bin/ccache \
    -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain \
    -DCMAKE_C_FLAGS_RELEASE="-O2 -UNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O2 -UNDEBUG" \
    ../ydb
   
    ```

## Build

To build all binary artifacts (server YDBD, client YDB, unittest binaries) run:
```bash
ninja
```

A YDB server binary can be found at:
```
ydb/apps/ydbd/ydbd
```

## Build and Test YDB CLI

To build YDB CLI (ydb):
```bash
ninja ydb/apps/ydb/all
```

A YDB CLI binary can be found at:
```
ydb/apps/ydb/ydb
```

### Unit tests

To build YDB CLI unit tests:
```bash
ninja ydb/public/lib/ydb_cli/all
```

To run tests execute:
```bash
cd ydb/public/lib/ydb_cli/
ctest
```

### Functional tests

Before launch tests you need to build YDB CLI and YDB server binaries. 
Also you can load [ydbd](https://ydb.tech/en/docs/downloads/#ydb-server) binary file and use it.
To launch YDB CLI python tests run `ydb_cli` test suite via pytest according to this [instruction](ydb/tests/functional/README.md).
