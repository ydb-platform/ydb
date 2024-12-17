[![Build and test](https://github.com/uriparser/uriparser/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/uriparser/uriparser/actions/workflows/build-and-test.yml)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/uriparseradmin/uriparser?svg=true)](https://ci.appveyor.com/project/uriparseradmin/uriparser)


# uriparser

uriparser is a
strictly [RFC 3986](http://tools.ietf.org/html/rfc3986) compliant
URI parsing and handling library
written in C89 ("ANSI C").
uriparser is cross-platform,
fast,
supports both `char` and `wchar_t`, and
is licensed under the [New BSD license](https://github.com/uriparser/uriparser/blob/master/COPYING).

To learn more about uriparser,
please check out [https://uriparser.github.io/](https://uriparser.github.io/).


# Example use from an existing CMake project

```cmake
cmake_minimum_required(VERSION 3.5.0)

project(hello VERSION 1.0.0)

find_package(uriparser 0.9.2 CONFIG REQUIRED char wchar_t)

add_executable(hello
    hello.c
)

target_link_libraries(hello PUBLIC uriparser::uriparser)
```


# Compilation

## Compilation (standalone, GNU make, Linux)
```console
# mkdir build
# cd build
# cmake -DCMAKE_BUILD_TYPE=Release ..  # see CMakeLists.txt for options
# make
# make test
# make install
```

## Available CMake options (and defaults)
```console
# rm -f CMakeCache.txt ; cmake -LH . | grep -B1 ':.*=' | sed 's,--,,'
// Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel ...
CMAKE_BUILD_TYPE:STRING=

// Install path prefix, prepended onto install directories.
CMAKE_INSTALL_PREFIX:PATH=/usr/local

// Path to qhelpgenerator program (default: auto-detect)
QHG_LOCATION:FILEPATH=

// Build code supporting data type 'char'
URIPARSER_BUILD_CHAR:BOOL=ON

// Build API documentation (requires Doxygen, Graphviz, and (optional) Qt's qhelpgenerator)
URIPARSER_BUILD_DOCS:BOOL=ON

// Build test suite (requires GTest >=1.8.0)
URIPARSER_BUILD_TESTS:BOOL=ON

// Build tools (e.g. CLI "uriparse")
URIPARSER_BUILD_TOOLS:BOOL=ON

// Build code supporting data type 'wchar_t'
URIPARSER_BUILD_WCHAR_T:BOOL=ON

// Enable installation of uriparser
URIPARSER_ENABLE_INSTALL:BOOL=ON

// Use of specific runtime library (/MT /MTd /MD /MDd) with MSVC
URIPARSER_MSVC_RUNTIME:STRING=

// Build shared libraries (rather than static ones)
URIPARSER_SHARED_LIBS:BOOL=ON

// Treat all compiler warnings as errors
URIPARSER_WARNINGS_AS_ERRORS:BOOL=OFF
```
