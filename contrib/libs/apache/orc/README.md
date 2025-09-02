# [Apache ORC](https://orc.apache.org/)

ORC is a self-describing type-aware columnar file format designed for
Hadoop workloads. It is optimized for large streaming reads, but with
integrated support for finding required rows quickly. Storing data in
a columnar format lets the reader read, decompress, and process only
the values that are required for the current query. Because ORC files
are type-aware, the writer chooses the most appropriate encoding for
the type and builds an internal index as the file is written.
Predicate pushdown uses those indexes to determine which stripes in a
file need to be read for a particular query and the row indexes can
narrow the search to a particular set of 10,000 rows. ORC supports the
complete set of types in Hive, including the complex types: structs,
lists, maps, and unions.

## ORC File Library

This project includes both a Java library and a C++ library for reading and writing the _Optimized Row Columnar_ (ORC) file format. The C++ and Java libraries are completely independent of each other and will each read all versions of ORC files.

Releases:

* Latest: [Apache ORC releases](https://orc.apache.org/releases)
* Maven Central: [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.orc/orc/badge.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.orc%22)
* Downloads: [Apache ORC downloads](https://orc.apache.org/downloads)
* Release tags: [Apache ORC release tags](https://github.com/apache/orc/releases)
* Plan: [Apache ORC future release plan](https://github.com/apache/orc/milestones)

The current build status:

* Main branch [![main build status](https://github.com/apache/orc/actions/workflows/build_and_test.yml/badge.svg?branch=main)](https://github.com/apache/orc/actions/workflows/build_and_test.yml?query=branch%3Amain)

Bug tracking: [Apache Jira](https://orc.apache.org/bugs)

The subdirectories are:

* c++ - the c++ reader and writer
* cmake_modules - the cmake modules
* docker - docker scripts to build and test on various linuxes
* examples - various ORC example files that are used to test compatibility
* java - the java reader and writer
* site - the website and documentation
* tools - the c++ tools for reading and inspecting ORC files

### Building

* Install java 17 or higher
* Install maven 3.9.9 or higher
* Install cmake 3.12 or higher

To build a release version with debug information:

```shell
% mkdir build
% cd build
% cmake ..
% make package
% make test-out

```

To build a debug version:

```shell
% mkdir build
% cd build
% cmake .. -DCMAKE_BUILD_TYPE=DEBUG
% make package
% make test-out

```

To build a release version without debug information:

```shell
% mkdir build
% cd build
% cmake .. -DCMAKE_BUILD_TYPE=RELEASE
% make package
% make test-out

```

To build only the Java library:

```shell
% cd java
% ./mvnw package

```

To build only the C++ library:

```shell
% mkdir build
% cd build
% cmake .. -DBUILD_JAVA=OFF
% make package
% make test-out

```

To build the C++ library with AVX512 enabled:

```shell
export ORC_USER_SIMD_LEVEL=AVX512
% mkdir build
% cd build
% cmake .. -DBUILD_JAVA=OFF -DBUILD_ENABLE_AVX512=ON
% make package
% make test-out
```

Cmake option BUILD_ENABLE_AVX512 can be set to "ON" or (default value)"OFF" at the compile time. At compile time, it defines the SIMD level(AVX512) to be compiled into the binaries.

Environment variable ORC_USER_SIMD_LEVEL can be set to "AVX512" or (default value)"NONE" at the run time. At run time, it defines the SIMD level to dispatch the code which can apply SIMD optimization.

Note that if ORC_USER_SIMD_LEVEL is set to "NONE" at run time, AVX512 will not take effect at run time even if BUILD_ENABLE_AVX512 is set to "ON" at compile time.

### Building with Meson

While CMake is the official build system for orc, there is unofficial support for using Meson to build select parts of the project. To build a debug version of the library and test it using Meson, from the project root you can run:

```shell
meson setup build
meson compile -C build
meson test -C build
```

By default, Meson will build unoptimized libraries with debug symbols. By contrast, the CMake build system generates release libraries by default. If you would like to create release libraries ala CMake, you should set the buildtype option. You must either remove the existing build directory before changing that setting, or alternatively pass the ``--reconfigure`` flag:

```shell
meson setup build -Dbuildtype=release --reconfigure
meson compile -C build
meson test -C build
```

Meson supports running your test suite through valgrind out of the box:

```shell
meson test -C build --wrap=valgrind
```

If you'd like to enable sanitizers, you can leverage the ``-Db_sanitize=`` option. For example, to enable both ASAN and UBSAN, you can run:

```shell
meson setup build -Dbuildtype=debug -Db_sanitize=address,undefined --reconfigure
meson compile -C build
meson test
```

Meson takes care of detecting all dependencies on your system, and downloading missing ones as required through its [Wrap system](https://mesonbuild.com/Wrap-dependency-system-manual.html). The dependencies for the project are all stored in the ``subprojects`` directory in individual wrap files. The majority of these are system generated files created by running:

```shell
meson wrap install <depencency_name>
```

From the project root. If you are developing orc and need to add a new dependency in the future, be sure to check Meson's [WrapDB](https://mesonbuild.com/Wrapdb-projects.html) to check if a pre-configured wrap entry exists. If not, you may still manually configure the dependency as outlined in the aforementioned Wrap system documentation.
