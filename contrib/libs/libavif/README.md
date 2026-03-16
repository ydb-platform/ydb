# libavif

This library aims to be a friendly, portable C implementation of the AV1 Image
File Format, as described here:

<https://aomediacodec.github.io/av1-avif/>

It can encode and decode all AV1 supported YUV formats and bit depths (with
alpha). In addition to the library, encoder and decoder command line tools are
also provided (`avifenc` and `avifdec`).

It is recommended that you check out/use
[tagged releases](https://github.com/AOMediaCodec/libavif/releases) instead of
just using the main branch. We will regularly create new versions as bug fixes
and features are added.

## Command line tool usage

```sh
avifenc -q 75 input.[jpg|png|y4m] output.avif
avifdec output.avif decoded.png
```

See `avifenc --help` for all options.

## API usage

Please see the examples in the "examples" directory. If you're already building
`libavif`, enable the CMake option `AVIF_BUILD_EXAMPLES` in order to build and
run the examples too.

## Installation

`libavif` is a package in most major OSs.

### Windows

```sh
vcpkg install libavif
```
You can also download the official windows binaries on the
[release](https://github.com/AOMediaCodec/libavif/releases) page.

### macOS

Homebrew:
```sh
brew install libavif
```
MacPorts:
```sh
sudo port install libavif
```

### Linux

Debian-based distributions:
```sh
sudo apt install libavif-dev
```
Red Hat-based distributions:
```sh
sudo yum -y install libavif
```

### MinGW

For the "default" MSYS2 UCRT64 environment:
```sh
pacman -S mingw-w64-ucrt-x86_64-libavif
```

## Build Notes

Building libavif requires [CMake](https://cmake.org/).
See [Build Command Lines](#build-command-lines) below for example command lines.

### Controlling Dependencies

CMake flags like `AVIF_CODEC_AOM`, `AVIF_LIBYUV`, etc. allow enabling or
disabling dependencies. They can take three possible values:
* `OFF`: the dependency is disabled.
* `SYSTEM`: the dependency is expected to be installed on the system.
* `LOCAL`: the dependency is built locally. In most cases, CMake can
  automatically download and build it. For some dependencies, you need to run the
  associated script in the `ext/` subdirectory yourself. In cases where
  CMake handles downloading the dependency, you can still call the script in
  `ext/` if you want to use a different version of the dependency (e.g. by
  modifying the script) or make custom code changes to it.
  If a directory with the dependency exists in the `ext/` directory, CMake will
  use it instead of downloading a new copy.

### Codec Dependencies

No AV1 codecs are enabled by default. You should enable at least one of them by
setting any of the following CMake options to `LOCAL` or `SYSTEM`, depending on
whether you want to use a locally built or a system installed version
(e.g. `-DAVIF_CODEC_AOM=LOCAL`):

* `AVIF_CODEC_AOM` for [libaom](https://aomedia.googlesource.com/aom/) (encoder
  and decoder)
* `AVIF_CODEC_DAV1D` for [dav1d](https://code.videolan.org/videolan/dav1d)
  (decoder)
* `AVIF_CODEC_LIBGAV1` for
  [libgav1](https://chromium.googlesource.com/codecs/libgav1/) (decoder)
* `AVIF_CODEC_RAV1E` for [rav1e](https://github.com/xiph/rav1e) (encoder)
* `AVIF_CODEC_SVT` for [SVT-AV1](https://gitlab.com/AOMediaCodec/SVT-AV1)
  (encoder)

When set to `SYSTEM`, these libraries (in their C API form) must be externally
available (discoverable via CMake's `FIND_LIBRARY`) to use them, or if libavif
is a child CMake project, the appropriate CMake target must already exist
by the time libavif's CMake scripts are executed.

### Libyuv Dependency

Libyuv is an optional but strongly recommended dependency that speeds up
color space conversions. It's enabled by default with a value of `SYSTEM`,
so it's expected to be installed on the system. It can either be built
locally instead by using `-DAVIF_LIBYUV=LOCAL` or disabled with
`-DAVIF_LIBYUV=OFF`.

### Tests

A few tests written in C can be built by enabling the `AVIF_BUILD_TESTS` CMake
option.

The remaining tests require [GoogleTest](https://github.com/google/googletest),
and can be built by enabling `AVIF_BUILD_TESTS` and setting `AVIF_GTEST` to
`SYSTEM` or `LOCAL`.

Additionally, fuzzing tests require [fuzztest](https://github.com/google/fuzztest),
see also fuzzing test instructions in `ext/oss-fuzz/README.md`.

Code coverage is available by enabling `AVIF_ENABLE_COVERAGE` then building
the `avif_coverage` target, e.g. `make avif_coverage -j`. It requires
compiling with clang (`-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++`)
and LLVM must be installed on the system.

### Build Command Lines

The following instructions can be used to build the libavif library and the
`avifenc` and `avifdec` tools.

#### Build using installed dependencies

To link against the already installed `aom`, `libjpeg`, `libpng` and `libyuv` dependency
libraries (recommended):

```sh
git clone -b v1.2.1 https://github.com/AOMediaCodec/libavif.git
cmake -S libavif -B libavif/build -DAVIF_CODEC_AOM=SYSTEM -DAVIF_BUILD_APPS=ON
cmake --build libavif/build --config Release --parallel
```

#### Build everything from scratch

For development and debugging purposes:

```sh
git clone -b v1.2.1 https://github.com/AOMediaCodec/libavif.git
cmake -S libavif -B libavif/build -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHARED_LIBS=OFF -DAVIF_CODEC_AOM=LOCAL -DAVIF_LIBYUV=LOCAL -DAVIF_LIBSHARPYUV=LOCAL -DAVIF_JPEG=LOCAL -DAVIF_ZLIBPNG=LOCAL -DAVIF_BUILD_APPS=ON
cmake --build libavif/build --config Debug --parallel
```

## Prebuilt Binaries (Windows)

Statically-linked `avifenc.exe` and `avifdec.exe` can be downloaded from the
[Releases](https://github.com/AOMediaCodec/libavif/releases) page.

## Development Notes

Please check the [wiki](https://github.com/AOMediaCodec/libavif/wiki) for extra
resources on libavif, such as the Release Checklist.

The libavif library is written in C99. Most of the tests are written in C++14.

### Formatting

Use [clang-format](https://clang.llvm.org/docs/ClangFormat.html) to format the
sources from the top-level folder (`clang-format-16` preferred):

```sh
clang-format -style=file -i \
  apps/*.c apps/*/*.c apps/*/*.cc apps/*/*.h examples/*.c \
  include/avif/*.h src/*.c src/*.cc \
  tests/*.c tests/*/*.cc tests/*/*.h
```

Use [cmake-format](https://github.com/cheshirekow/cmake_format) to format the
CMakeLists.txt files from the top-level folder:

```sh
cmake-format -i \
  CMakeLists.txt \
  tests/CMakeLists.txt \
  cmake/Modules/*.cmake \
  contrib/CMakeLists.txt \
  contrib/gdk-pixbuf/CMakeLists.txt \
  android_jni/avifandroidjni/src/main/jni/CMakeLists.txt
```

---

## License

Released under the BSD License.

```markdown
Copyright 2019 Joe Drago. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```
