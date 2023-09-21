# Building YDB from sources

From this repository you can build YDB Server and YDB CLI (Command Line Interface utility) executables.

## Build Requirements

Only x86_64 architecture is currently supported.
YDB server can be built for Ubuntu 18.04, 20.04 and 22.04. Other Linux distributions are likely to work, but additional effort may be needed.
YDB CLI can be built for Ubuntu 18+, Windows, and MacOS. Instructions below are provided for Ubuntu only, other options are to be described later.

## Prerequisites

Below is a list of packages that need to be installed before building YDB. [How to Build](#how-to-build) section contains step by step instructions to obtain these packages.

 - cmake 3.22+
 - clang-14
 - lld-14
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

## Additional steps for Ubuntu versions earlier than 22.04

It is recommended to build YDB on Ubuntu 22.04. Follow these additional instructions if you don't have access to it and need to use an earlier version.

<details>
   <summary>For Ubuntu 18.04, install Python 3.8, create and activate a new virtual environment, and install the latest PIP.</summary>

   ```bash
   apt-get install python3.8 python3.8-venv python3-venv
   python3.8 -m venv ~/ydbwork/ve
   source ~/ydbwork/ve/bin/activate
   pip install -U pip
   ```
</details>

<details>
   <summary>For Ubuntu 18.04 and Ubuntu 20.04, add CMake and LLVM APT repositories.</summary>

   ```bash
   wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | sudo apt-key add -
   echo "deb http://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
   
   wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
   echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-14 main" | sudo tee /etc/apt/sources.list.d/llvm.list >/dev/null
   
   sudo apt-get update
   
   ```

</details>

## Install dependencies

```bash
sudo apt-get -y install git cmake python3-pip ninja-build antlr3 m4 clang-14 lld-14 libidn11-dev libaio1 libaio-dev llvm-14
sudo pip3 install conan==1.59 grpcio-tools==1.57.0

```

## Create the work directory. 
> :warning: Please make sure you have at least 80Gb of free space. We also recommend placing this directory on SSD to reduce build times.

```bash
mkdir ~/ydbwork && cd ~/ydbwork
mkdir build
```

## Install ccache

1. Install `ccache` into `/usr/local/bin/`. The recommended version is `4.8.1` or above, the minimum required version is `4.7`.
    ```bash
    (V=4.8.1; curl -L https://github.com/ccache/ccache/releases/download/v${V}/ccache-${V}-linux-x86_64.tar.xz | \
     sudo tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${V}-linux-x86_64/ccache)
    ```

2. Configure `ccache` to use remote storage of YDB build artifacts to speed up the first build time:
    ```bash
    ccache -o remote_storage="http://cachesrv.ydb.tech:8080|read-only|layout=bazel"
    ccache -o sloppiness=locale 
    ccache -o base_dir=~/ydbwork/
    ```

If you use a non-default work directory, adjust the `base_dir` ccache option to match it.

## Clone the ydb repository.

```bash
git clone https://github.com/ydb-platform/ydb.git
```

By default, the 'main' branch is checked out. It contains the latest development update for both YDB Server and CLI. This branch may sometimes be broken for short periods of time, so you may prefer to build the latest stable versions of YDB Server and CLI as described below.

### Check out the latest stable YDB Server version for build

To build the latest stable version of a YDB Server, check out the latest stable Server tag from the repository. To do so, visit the https://github.com/ydb-platform/ydb/releases/latest page and use the provided 'tag' value in the `git checkout <tag>` command.

For example, at the time of writing the latest stable release was [YDB Server 23.2.12](https://github.com/ydb-platform/ydb/releases/tag/23.2.12) with a tag `23.2.12`, so the command looks like that:

```bash
git checkout 23.2.12
```

### Check out the latest stable YDB CLI version for build

To build a latest stable version of a YDB CLI, check out the latest stable CLI tag from the repository. To do so, visit the https://github.com/ydb-platform/ydb/releases page, scroll down to the top-most 'YDB CLI' release, and use the provided 'tag' value in the `git checkout <tag>` command.

For example, at the time of writing the latest YDB CLI release was [YDB CLI 2.5.0](https://github.com/ydb-platform/ydb/releases/tag/CLI_2.5.0) with a tag `CLI_2.5.0`, so the command looks like that:

```bash
git checkout CLI_2.5.0
```

## Configure

1. Change Conan's home folder to the build folder for better remote cache hit 
    ```bash
    export CONAN_USER_HOME=~/ydbwork/build
    ```

2. Generate build configuration using `ccache`
    ```bash
    cd build
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DCCACHE_PATH=/usr/local/bin/ccache \
    -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain \
    ../ydb  
    ```

## Build 

### Build YDB Server

To build YDB Server run:
```bash
ninja ydb/apps/ydbd/all
```

A YDB server binary can be found at:
```
ydb/apps/ydbd/ydbd
```

### Build YDB CLI

To build YDB CLI (Command Line Interface utility) run:
```bash
ninja ydb/apps/ydb/all
```

A YDB CLI binary can be found at:
```
ydb/apps/ydb/ydb
```

## Run tests

### Build all executable artifacts

To run tests, you first to build all binary artifacts (tools, test executables, server and CLI, etc.), running `ninja` without parameters:
```bash
ninja
```

### Run unit tests

To run tests execute:
```bash
ctest
```

### Functional tests

Run test suites via pytest according to this [instruction](ydb/tests/functional/README.md).

## Run YDB CLI tests only

You may choose building and running only YDB CLI tests when changing the YDB CLI code, as it is faster than running the full YDB test suite.

To build YDB CLI binary and YDB CLI unit tests binaries run:
```bash
ninja ydb/apps/ydb/all
ninja ydb/public/lib/ydb_cli/all
```

To run YDB CLI unit tests execute:
```bash
cd ydb/public/lib/ydb_cli/
ctest
```

To run YDB CLI functional tests you need a compiled YDB Server binary (ydbd) located at ~/ydbwork/ydb/apps/ydbd/ydbd. You may compile it as described in the [Build YDB Server](#build-ydb-server) above, or download a precompiled binary from the YDB [Downloads](https://ydb.tech/en/docs/downloads/#ydb-server) page.

When both YDB CLI and YDB Server binary artifacts are present, launch the `ydb_cli` test suite via pytest according to this [instruction](ydb/tests/functional/README.md).
