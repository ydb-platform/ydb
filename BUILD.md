## Building YDB from sources

#### Build Requirements
 We have tested YDB builds using Ubuntu 18.04 and Ubuntu 20.04. Other Linux distributions are likely to work, but additional effort may be needed. Only x86_64 Linux is currently supported.

 Below is a list of packages that need to be installed before building YDB. 'How to Build' section contains step by step instructions to obtain these packages.

 - cmake 3.22+
 - clang-12
 - lld-12
 - git 2.20+
 - python2.7
 - python3.8
 - pip3
 - antlr3
 - libaio-dev
 - libidn11-dev
 - ninja 1.10+

 We run multiple clang instances in parallel to speed up the process by default. Each instance of clang may use up to 1GB of RAM, and linking the binary may use up to 16GB of RAM, please make sure your build host has enough resources.

#### Runtime Requirements
 The following packages are required to run ydbd server:

 - libidn11
 - libaio

#### How to Build

1. Add repositories for dependencies

    Note: the following repositories are required for **Ubuntu 18.04 and Ubuntu 20.04**. You may skip this step if your GNU/Linux distribution has all required packages in their default repository.
    For more information please read your distribution documentation and https://apt.llvm.org as well as https://apt.kitware.com/
     ```
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
    wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | sudo apt-key add -
    echo "deb http://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
    echo "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/llvm.list >/dev/null

    sudo apt-get update
     ```

   For Ubuntu 18, use `llvm-toolchain-$(lsb_release -cs)-12` in the command above.

1. Install dependencies

    ```
    sudo apt-get -y install git cmake python3-pip ninja-build antlr3 m4 clang-12 lld-12 libidn11-dev libaio1 libaio-dev
    sudo pip3 install conan==1.59
     ```

 1. Create the work directory. Please make sure you have at least 80Gb of free space. We also recommend placing this directory on SSD to reduce build times.
    ```
    mkdir ~/ydbwork && cd ~/ydbwork
    mkdir build
    ```

 1. Clone the ydb repository
    ```
    git clone https://github.com/ydb-platform/ydb.git
    ```

 1. Build ydb

    Run cmake to generate build configuration:

    ```
    cd build
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain ../ydb
    ```

    To build both YDB server (ydbd) and YDB CLI (ydb) run:
    ```
    ninja
    ```

    To build only YDB CLI (ydb) run:
    ```
    ninja ydb/apps/ydb/all
    ```

    A YDB server binary can be found at:
    ```
    ydb/apps/ydbd/ydbd
    ```
    A YDB CLI binary can be found at:
    ```
    ydb/apps/ydb/ydb
    ```

#### How to Run

1. Start server

    ```
    YDBD_PATH=ydb/apps/ydbd/ydbd ../ydb/ydb/deploy/local_binary/linux/start.sh ram
    ```

    or

    ```
    YDBD_PATH=ydb/apps/ydbd/ydbd ../ydb/ydb/deploy/local_binary/linux/start.sh disk
    ```

2. Test connection with client

    ```
    ydb/apps/ydb/ydb -e grpc://localhost:2136 -d /Root scheme ls # should return "test .sys"
    ```

3. Enjoy!

4. Stop server

    ```
    YDBD_PATH=ydb/apps/ydbd/ydbd ../ydb/ydb/deploy/local_binary/linux/stop.sh
    ```