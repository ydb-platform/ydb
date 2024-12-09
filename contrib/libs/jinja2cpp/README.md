<div align="center"><img width="200" src="https://avatars0.githubusercontent.com/u/49841676?s=200&v=4"></div>

# Jinja2С++

[![Language](https://img.shields.io/badge/language-C++-blue.svg)](https://isocpp.org/)
[![Standard](https://img.shields.io/badge/c%2B%2B-14-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B#Standardization)
[![Standard](https://img.shields.io/badge/c%2B%2B-17-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B#Standardization)
[![Standard](https://img.shields.io/badge/c%2B%2B-20-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B#Standardization)
[![Coverage Status](https://codecov.io/gh/jinja2cpp/Jinja2Cpp/branch/master/graph/badge.svg)](https://codecov.io/gh/jinja2cpp/Jinja2Cpp)
[![Github Releases](https://img.shields.io/github/release/jinja2cpp/Jinja2Cpp/all.svg)](https://github.com/jinja2cpp/Jinja2Cpp/releases)
[![Github Issues](https://img.shields.io/github/issues/jinja2cpp/Jinja2Cpp.svg)](http://github.com/jinja2cpp/Jinja2Cpp/issues)
[![GitHub License](https://img.shields.io/badge/license-Mozilla-blue.svg)](https://raw.githubusercontent.com/jinja2cpp/Jinja2Cpp/master/LICENSE)
[![conan.io](https://api.bintray.com/packages/conan/conan-center/jinja2cpp%3A_/images/download.svg?version=1.2.1%3A_) ](https://conan.io/center/jinja2cpp)
[![Gitter Chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/Jinja2Cpp/Lobby)

C++ implementation of the Jinja2 Python template engine. This library brings support of powerful Jinja2 template features into the C++ world, reports  dynamic HTML pages and source code generation.

## Introduction

Main features of Jinja2C++:
-  Easy-to-use public interface. Just load templates and render them.
-  Conformance to [Jinja2 specification](http://jinja.pocoo.org/docs/2.10/)
-  Full support of narrow- and wide-character strings both for templates and parameters.
-  Built-in reflection for the common C++ types, nlohmann and rapid JSON libraries.
-  Powerful full-featured Jinja2 expressions with filtering (via '|' operator) and 'if'-expressions.
-  Control statements (`set`, `for`, `if`, `filter`, `do`, `with`).
-  Templates extension, including and importing
-  Macros
-  Rich error reporting.
-  Shared template environment with templates cache support

For instance, this simple code:

```c++
#include <jinja2cpp/template.h>

std::string source = R"(
{{ ("Hello", 'world') | join }}!!!
{{ ("Hello", 'world') | join(', ') }}!!!
{{ ("Hello", 'world') | join(d = '; ') }}!!!
{{ ("Hello", 'world') | join(d = '; ') | lower }}!!!
)";

Template tpl;
tpl.Load(source);

std::string result = tpl.RenderAsString({}).value();
```

produces the result string:

```
Helloworld!!!
Hello, world!!!
Hello; world!!!
hello; world!!!
```

## Getting started

To use Jinja2C++ in your project you have to:
* Clone the Jinja2C++ repository
* Build it according to the [instructions](https://jinja2cpp.github.io/docs/build_and_install.html)
* Link to your project.

Usage of Jinja2C++ in the code is pretty simple:
1.  Declare the jinja2::Template object:

```c++
jinja2::Template tpl;
```

2.  Populate it with template:

```c++
tpl.Load("{{ 'Hello World' }}!!!");
```

3.  Render the template:

```c++
std::cout << tpl.RenderAsString({}).value() << std::endl;
```

and get:

`
Hello World!!!
`

That's all!

More detailed examples and features description can be found in the documentation: [https://jinja2cpp.github.io/docs/usage](https://jinja2cpp.github.io/docs/usage)

## Current Jinja2 support
Currently, Jinja2C++ supports the limited number of Jinja2 features. By the way, Jinja2C++ is planned to be a fully [jinja2 specification](http://jinja.pocoo.org/docs/2.10/templates/)-conformant. The current support is limited to:
-  expressions. You can use almost every expression style: simple, filtered, conditional, and so on.
-  the big number of filters (**sort, default, first, last, length, max, min, reverse, unique, sum, attr, map, reject, rejectattr, select, selectattr, pprint, dictsort, abs, float, int, list, round, random, trim, title, upper, wordcount, replace, truncate, groupby, urlencode, capitalize, escape, tojson, striptags, center, xmlattr**)
-  the big number of testers (**eq, defined, ge, gt, iterable, le, lt, mapping, ne, number, sequence, string, undefined, in, even, odd, lower, upper**)
-  the number of functions (**range**, **loop.cycle**)
-  'if' statement (with 'elif' and 'else' branches)
-  'for' statement (with 'else' branch and 'if' part support)
-  'include' statement
-  'import'/'from' statements
-  'set' statement (both line and block)
-  'filter' statement
-  'extends'/'block' statements
-  'macro'/'call' statements
-  'with' statement
-  'do' extension statement
-  recursive loops
-  space control and 'raw'/'endraw' blocks

Full information about Jinja2 specification support and compatibility table can be found here: [https://jinja2cpp.github.io/docs/j2_compatibility.html](https://jinja2cpp.github.io/docs/j2_compatibility.html).

## Supported compilers
Compilation of Jinja2C++ tested on the following compilers (with C++14 and C++17 enabled features):
-  Linux gcc 5.5 - 9.0
-  Linux clang 5.0 - 9
-  MacOS X-Code 9
-  MacOS X-Code 10
-  MacOS X-Code 11 (C++14 in default build, C++17 with externally-provided boost)
-  Microsoft Visual Studio 2015 - 2019 x86, x64
-  MinGW gcc compiler 7.3
-  MinGW gcc compiler 8.1

**Note:** Support of gcc version >= 9.x or clang version >= 8.0 depends on the version of the Boost library provided.

### Build status

| Compiler | Status  |
|---------|---------:|
| **MSVC** 2015 (x86, x64), **MinGW** 7 (x64), **MinGW** 8 (x64) | [![Build status](https://ci.appveyor.com/api/projects/status/vu59lw4r67n8jdxl/branch/master?svg=true)](https://ci.appveyor.com/project/flexferrum/jinja2cpp-n5hjm/branch/master) |
| **X-Code** 9, 10, 11  | [![Build Status](https://travis-ci.org/jinja2cpp/Jinja2Cpp.svg?branch=master)](https://travis-ci.org/jinja2cpp/Jinja2Cpp) |
| **MSVC** 2017 (x86, x64), **MSVC** 2019 (x86, x64), C++14/C++17 | [![](https://github.com/jinja2cpp/Jinja2Cpp/workflows/CI-windows-build/badge.svg)](https://github.com/jinja2cpp/Jinja2Cpp/actions?query=workflow%3ACI-windows-build) |
| **g++** 5, 6, 7, 8, 9, 10, 11 **clang** 5, 6, 7, 8, 9, 10, 11, 12 C++14/C++17/C++20 | [![](https://github.com/jinja2cpp/Jinja2Cpp/workflows/CI-linux-build/badge.svg)](https://github.com/jinja2cpp/Jinja2Cpp/actions?query=workflow%3ACI-linux-build) |

## Build and install
Jinja2C++ has several external dependencies:
-  `boost` library (at least version 1.65)
-  `nonstd::expected-lite` [https://github.com/martinmoene/expected-lite](https://github.com/martinmoene/expected-lite)
-  `nonstd::variant-lite` [https://github.com/martinmoene/variant-lite](https://github.com/martinmoene/variant-lite)
-  `nonstd::optional-lite` [https://github.com/martinmoene/optional-lite](https://github.com/martinmoene/optional-lite)
-  `nonstd::string-view-lite` [https://github.com/martinmoene/string-view-lite](https://github.com/martinmoene/string-view-lite)
-  `fmtlib::fmt` [https://github.com/fmtlib/fmt](https://github.com/fmtlib/fmt)

Examples of build scripts and different build configurations could be found here: [https://github.com/jinja2cpp/examples-build](https://github.com/jinja2cpp/examples-build)

In simplest case to compile Jinja2C++ you need:

1.  Install CMake build system (at least version 3.0)
2.  Clone jinja2cpp repository:

```
> git clone https://github.com/flexferrum/Jinja2Cpp.git
```

3.  Create build directory:

```
> cd Jinja2Cpp
> mkdir build
```

4.  Run CMake and build the library:

```
> cd build
> cmake .. -DCMAKE_INSTALL_PREFIX=<path to install folder>
> cmake --build . --target all
```
"Path to install folder" here is a path to the folder where you want to install Jinja2C++ lib.

5. Install library:

```
> cmake --build . --target install
```

In this case, Jinja2C++ will be built with internally-shipped dependencies and install them respectively. But Jinja2C++ supports builds with externally-provided deps.
### Usage with conan.io dependency manager
Jinja2C++ can be used as conan.io package. In this case, you should do the following steps:

1. Install conan.io according to the documentation ( https://docs.conan.io/en/latest/installation.html )
2. Add a reference to Jinja2C++ package (`jinja2cpp/1.2.1`) to your conanfile.txt, conanfile.py or CMakeLists.txt. For instance, with the usage of `conan-cmake` integration it could be written this way:

```cmake

cmake_minimum_required(VERSION 3.24)
project(Jinja2CppSampleConan CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR})

add_definitions("-std=c++14")

if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
  message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
  file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/0.18.1/conan.cmake"
                "${CMAKE_BINARY_DIR}/conan.cmake"
                TLS_VERIFY ON)
endif()
include(${CMAKE_BINARY_DIR}/conan.cmake)

conan_cmake_autodetect(settings)
conan_cmake_run(REQUIRES
                    jinja2cpp/1.1.0
                    gtest/1.14.0
                BASIC_SETUP
                ${CONAN_SETTINGS}
                OPTIONS
                    jinja2cpp/*:shared=False
                    gtest/*:shared=False
                BUILD missing)

set (TARGET_NAME jinja2cpp_build_test)

add_executable (${TARGET_NAME} main.cpp)

target_link_libraries (${TARGET_NAME} ${CONAN_LIBS})
set_target_properties (${TARGET_NAME} PROPERTIES
            CXX_STANDARD 14
            CXX_STANDARD_REQUIRED ON)

```


### Additional CMake build flags
You can define (via -D command-line CMake option) the following build flags:

-  **JINJA2CPP_BUILD_TESTS** (default TRUE) - to build or not to Jinja2C++ tests.
-  **JINJA2CPP_STRICT_WARNINGS** (default TRUE) - Enable strict mode compile-warnings(-Wall -Werror, etc).
-  **JINJA2CPP_MSVC_RUNTIME_TYPE** (default /MD) - MSVC runtime type to link with (if you use Microsoft Visual Studio compiler).
-  **JINJA2CPP_DEPS_MODE** (default "internal") - modes for dependency handling. Following values possible:
    -  `internal` In this mode Jinja2C++ build script uses dependencies (include `boost`) shipped as subprojects. Nothing needs to be provided externally.
    -  `external-boost` In this mode Jinja2C++ build script uses only `boost` as an externally-provided dependency. All other dependencies are taken from subprojects.
    -  `external` In this mode all dependencies should be provided externally. Paths to `boost`, `nonstd-*` libs, etc. should be specified via standard CMake variables (like `CMAKE_PREFIX_PATH` or libname_DIR)
    -  `conan-build` Special mode for building Jinja2C++ via conan recipe.


### Build with C++17 standard enabled
Jinja2C++ tries to use standard versions of `std::variant`, `std::string_view` and `std::optional` if possible.

## Acknowledgments
Thanks to **@flexferrum** for creating this library, for being one of the brightest minds in software engineering community. Rest in peace, friend.

Thanks to **@manu343726** for CMake scripts improvement, bug hunting, and fixing and conan.io packaging.

Thanks to **@martinmoene** for the perfectly implemented xxx-lite libraries.

Thanks to **@vitaut** for the amazing text formatting library.

Thanks to **@martinus** for the fast hash maps implementation.


## Changelog


### Version 1.3.1

#### Changes and improvements
- bump deps versions
- add new json binding - boost::json
- speedup regex parsing by switching to boost::regex(std::regex extremely slow)
    - templates are now loading faster

#### Fixed bugs
- small fixes across code base

#### Breaking changes
- internal deps now used through cmake fetch_content
- default json serializer/deserializer is switched to boost::json

### Version 1.2.1

#### Changes and improvements
- bump deps versions
- support modern compilers(up to Clang 12) and standards(C++20)
- tiny code style cleanup

#### Fixed bugs
- small fixes across code base

#### Breaking changes
- internal deps point to make based boost build

### Version 1.1.0
#### Changes and improvements
- `batch` filter added
- `slice` filter added
- `format` filter added
- `tojson` filter added
- `striptags` filter added
- `center` filter added
- `xmlattr` filter added
- `raw`/`endraw` tags added
- repeat string operator added (e. g. `'a' * 5` will produce `'aaaaa'`)
- support for templates metadata (`meta`/`endmeta` tags) added
- `-fPIC` flag added to Linux build configuration

#### Fixed bugs
- Fix behavior of lstripblock/trimblocks global settings. Now it fully corresponds to the origina jinja2
- Fix bug with rendering parent `block` content if child doesn't override this block
- Fix compilation issues with user-defined callables with number of arguments more than 2
- Fix access to global Jinja2 functions from included/extended templates
- Fix point of evaluation of macro params
- Fix looping over the strings
- Cleanup warnings

#### Breaking changes
- From now with C++17 standard enabled Jinja2C++ uses standard versions of types `variant`, `string_view` and `optional`

### Version 1.0.0
#### Changes and improvements
- `default` attribute added to the `map` filter (#48)
- escape sequences support added to the string literals (#49)
- arbitrary ranges, generated sequences, input iterators, etc. now can be used with `GenericList` type (#66)
- nonstd::string_view is now one of the possible types for the `Value`
- `filter` tag support added to the template parser (#44)
- `escape` filter support added to the template parser (#140)
- `capitalize` filter support added to the template parser (#137)
- the multiline version of `set` tag added to the parser (#45)
- added built-in reflection for nlohmann JSON and RapidJSON libraries (#78)
- `loop.depth` and `loop.depth0` variables support added
- {fmt} is now used as a formatting library instead of iostreams
- robin hood hash map is now used for internal value storage
- rendering performance improvements
- template cache implemented in `TemplateEnv`
- user-defined callables now can accept global context via `*context` special param
- MinGW, clang >= 7.0, XCode >= 9, gcc >= 7.0 are now officially supported as a target compilers (#79)

#### Fixed bugs
- Fixed pipe (`|`) operator precedence (#47)
- Fixed bug in internal char <-> wchar_t converter on Windows
- Fixed crash in parsing `endblock` tag
- Fixed scope control for `include` and `for` tags
- Fixed bug with macros call within expression context

#### Breaking changes
- MSVC runtime type is now defined by `JINJA2CPP_MSVC_RUNTIME_TYPE` CMake variable

### Version 0.9.2
#### Major changes
- User-defined callables implemented. Now you can define your own callable objects, pass them as input parameters and use them inside templates as regular (global) functions, filters or testers. See details here: https://jinja2cpp.github.io/docs/usage/ud_callables.html
- Now you can define global (template environment-wide) parameters that are accessible for all templates bound to this environment.
- `include`, `import` and `from` statements implemented. Now it's possible to include other templates and use macros from other templates.
- `with` statement implemented
- `do` statement implemented
- Sample build projects for various Jinja2C++ usage variants created: https://github.com/jinja2cpp/examples-build](https://github.com/jinja2cpp/examples-build)
- Documentation site created for Jinja2C++: https://jinja2cpp.github.io

#### Minor changes
- Render-time error handling added
- Dependency management mode added to the build script
- Fix bugs with error reporting during the parse time
- Upgraded versions of external dependencies

#### Breaking changes
- `RenderAsString` method now returns `nonstd::expected` instead of regular `std::string`
- Templates with `import`, `extends` and `include` generate errors if parsed without `TemplateEnv` set
- Release bundles (archives) are configured with `external` dependency management mode by default

### Version 0.9.1
-  `applymacro` filter added which allows applying arbitrary macro as a filter
-  dependencies to boost removed from the public interface
-  CMake scripts improved
-  Various bugs fixed
-  Improve reflection
-  Warnings cleanup

### Version 0.9
-  Support of 'extents'/'block' statements
-  Support of 'macro'/'call' statements
-  Rich error reporting
-  Support for recursive loops
-  Support for space control before and after control blocks
-  Improve reflection

### Version 0.6
-  A lot of filters have been implemented. Full set of supported filters listed here: [https://github.com/flexferrum/Jinja2Cpp/issues/7](https://github.com/flexferrum/Jinja2Cpp/issues/7)
-  A lot of testers have been implemented. Full set of supported testers listed here: [https://github.com/flexferrum/Jinja2Cpp/issues/8](https://github.com/flexferrum/Jinja2Cpp/issues/8)
-  'Concatenate as string' operator ('~') has been implemented
-  For-loop with 'if' condition has been implemented
-  Fixed some bugs in parser
