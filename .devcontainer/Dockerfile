FROM --platform=linux/amd64 mcr.microsoft.com/devcontainers/cpp:1-ubuntu-22.04

# Install C++ tools and libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    gdb \
    git \
    python3-pip \
    ninja-build \
    antlr3 \
    m4 \
    clang-14 \
    lld-14 \
    libidn11-dev \
    libaio1 \
    libaio-dev \
    llvm-14 \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install ccache 4.8.1 or above
ENV CCACHE_VERSION=4.8.1
RUN wget https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz \
    && tar -xf ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz \
    && cp ccache-${CCACHE_VERSION}-linux-x86_64/ccache /usr/local/bin/ \
    && rm -rf ccache-${CCACHE_VERSION}-linux-x86_64 ccache-${CCACHE_VERSION}-linux-x86_64.tar.xz

# Install Python packages
RUN pip3 install conan==1.59 grpcio-tools==1.57.0
