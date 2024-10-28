# ANTLRv4 C3 C++ Port

## About

This is a port of the `antlr4-c3` library to `C++`.

Please see the parent [README.md](../../readme.md) for an explanation of the library and for examples.

## Port features

1. Only `CodeCompletionCore` was ported.

2. Supports cancellation for `collectCandidates` method via timeout or flag.

## Requirements

- [C++ 20 standard](https://en.cppreference.com/w/cpp/20) to compile sources.

- [ANTLRv4 C++ Runtime](https://github.com/antlr/antlr4/tree/4.13.1/runtime/Cpp) to compile sources.

- [CMake 3.7](https://cmake.org/cmake/help/latest/release/3.7.html) to build project.

- [ANTLRv4 Tool](https://www.antlr.org/download.html) to build tests.

- [Google Test](https://github.com/google/googletest) to build tests.

## Usage

Currently, there are no other ways to adding C++ port as a dependency other than by copying and pasting the [directory with project's source code](./source/antlr4-c3) into your own project.

## Build & Run

Actual build steps are available at [CMake GitHub Workflow](../../.github/workflows/cmake.yml). 

`ANTLRv4` Runtime and Tool as well as other dependecnies will be downloaded during `CMake` configiration stage.

```bash
# Clone antlr4-c3 repository and enter C++ port directory
git clone git@github.com:mike-lischke/antlr4-c3.git
cd antlr4-c3/ports/cpp # Also a workspace directory for VSCode

# Create and enter the build directory
mkdir build && cd build

# Configure CMake build
# - ANTLR4C3_DEVELOPER should be enabled if you are going to run tests
# - CMAKE_BUILD_TYPE Asan and Tsan are supported too
cmake -DANTLR4C3_DEVELOPER=ON -DCMAKE_BUILD_TYPE=Release ..

# Build everything
make

# Running tests being at build directory
(make && cd test && ctest)
```

## Contributing

We recommend using [VSCode](https://code.visualstudio.com/) with [clangd extension](https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd) as an IDE. There are some configuration files for launching tests in debug mode, `clangd` configuration and more.
