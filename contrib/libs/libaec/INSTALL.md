# Getting libaec

The source code of libaec is hosted at DKRZ GitLab.

## Source code and binary releases

The latest releases of libaec can be downloaded at the following
locations:

  https://github.com//Deutsches-Klimarechenzentrum/libaec/releases

or

  https://gitlab.dkrz.de/dkrz-sw/libaec/-/releases

## Developer snapshot

```shell
  git clone https://github.com//Deutsches-Klimarechenzentrum/libaec
```

# Installation

## General considerations

Libaec achieves the best performance on 64 bit systems. The library
will work correctly on 32 bit systems but encoding and decoding
performance will be much lower.

## Installation from source code release with configure

The most common installation procedure on Unix-like systems looks as
follows:

Unpack the tar archive and change into the unpacked directory.

```shell
  mkdir build
  cd build
  ../configure
  make check install
```

## Installation from source code release with CMake

As an alternative, you can use CMake to install libaec.

Unpack the tar archive and change into the unpacked directory.

```shell
  mkdir build
  cd build
  cmake ..
  make install
```

You can set options for compiling using the CMake GUI by replacing the cmake
command with

```shell
  cmake-gui ..
```

or by setting the options manually, e.g.

```shell
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=~/local ..
```

CMake can also generate project files for Microsoft Visual Studio when
used in Windows.

## Installation from cloned repository

The configure script is not included in the repository. You can
generate it with autotools and gnulib:

```shell
  cd libaec
  gnulib-tool --import lib-symbol-visibility
  autoreconf -iv
  mkdir build
  cd build
  ../configure
  make check install
```

# Optimization

Libaec in general and encoding performance in particular can benefit
from vectorization and other compiler optimizations. You can try to
enable higher than default optimizations and check the benefits with
the bench target.

## Intel compiler
Assuming your CPU supports AVX2, the following options will increase
encoding speed.

```shell
  ../configure CC=icc
  make CFLAGS="-O3 -xCORE-AVX2" bench
```

On a 3.4 GHz E3-1240 v3 we see more than 400 MiB/s for encoding
typical data.

## gcc
The default -O2 will already enable vectorization but -O3 yields even
better performance.
