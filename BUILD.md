## Build YDB from source

##### Requirements:
 - cmake 3.22+
 - clang-12
 - lld-12
 - git 2.20+
 - python2.7
 - python3.8
 - pip3
 - antlr3
 - libaio
 - ninja 1.10+

##### How to build:
 Currently x86_64 linux is supported. Building was tested on Ubuntu 20.04
 Note when multithreading build is on operation each instance of clang can use up to 1 GB of RAM. Linkage of binary file can use up to 16Gb ram. Please make sure system has enough memory.

1. Add repositories to install dependencies

    Note following repositories is required for **ubuntu 20.04**. If your GNU/Linux distributive already have required build dependencies you need to skip this step.
    For more information look in to your distributive instruction and https://apt.llvm.org and https://apt.kitware.com/
     ```
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key|sudo apt-key add -
    wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc |sudo apt-key add -
    echo 'deb http://apt.kitware.com/ubuntu/ focal main' | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
    echo 'deb http://apt.llvm.org/focal/ llvm-toolchain-focal main' | sudo tee /etc/apt/sources.list.d/llvm.list >/dev/null

    sudo apt-get update
     ```

1. Install dependencies

    ```
    sudo apt-get -y install git cmake python python3-pip ninja-build antlr3 m4 clang-12 lld-12 libaio1 libaio-dev
    sudo pip3 install conan
     ```

 1. Create work directory. Please make sure youre have at least 80Gb free space on the filesystem where your want to place this directory. Also we recomend to use SSD drive to reduce build time.
    ```
    mkdir ~/ydbwork && cd ~/ydbwork
    mkdir build
    ```

 1. Checkout ydb repository.
    ```
    git clone https://github.com/ydb-platform/ydb.git
    ```

 1. Build ydb
     ```
    cd build
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../ydb/clang.toolchain ../ydb
    ninja
    ```
    The YDB server binary can be found at:
    ```
    ydb/apps/ydbd/ydbd
    ```
    The YDB CLI binart at:
    ```
    ydb/apps/ydb/ydb
    ```
