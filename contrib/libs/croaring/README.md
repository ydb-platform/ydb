# CRoaring

[![Ubuntu-CI](https://github.com/RoaringBitmap/CRoaring/actions/workflows/ubuntu-noexcept-ci.yml/badge.svg)](https://github.com/RoaringBitmap/CRoaring/actions/workflows/ubuntu-noexcept-ci.yml) [![VS17-CI](https://github.com/RoaringBitmap/CRoaring/actions/workflows/vs17-ci.yml/badge.svg)](https://github.com/RoaringBitmap/CRoaring/actions/workflows/vs17-ci.yml)
[![Fuzzing Status](https://oss-fuzz-build-logs.storage.googleapis.com/badges/croaring.svg)](https://bugs.chromium.org/p/oss-fuzz/issues/list?sort=-opened&can=1&q=proj:croaring)

[![Doxygen Documentation](https://img.shields.io/badge/docs-doxygen-green.svg)](http://roaringbitmap.github.io/CRoaring/)



Portable Roaring bitmaps in C (and C++) with full support for your favorite compiler (GNU GCC, LLVM's clang, Visual Studio, Apple Xcode, Intel oneAPI). Included in the [Awesome C](https://github.com/kozross/awesome-c) list of open source C software.

# Introduction

Bitsets, also called bitmaps, are commonly used as fast data structures. Unfortunately, they can use too much memory.
 To compensate, we often use compressed bitmaps.

Roaring bitmaps are compressed bitmaps which tend to outperform conventional compressed bitmaps such as WAH, EWAH or Concise.
They are used by several major systems such as [Apache Lucene][lucene] and derivative systems such as [Solr][solr] and
[Elasticsearch][elasticsearch], [Metamarkets' Druid][druid], [LinkedIn Pinot][pinot], [Netflix Atlas][atlas], [Apache Spark][spark], [OpenSearchServer][opensearchserver], [Cloud Torrent][cloudtorrent], [Whoosh][whoosh], [InfluxDB](https://www.influxdata.com), [Pilosa][pilosa], [Bleve](http://www.blevesearch.com), [Microsoft Visual Studio Team Services (VSTS)][vsts], and eBay's [Apache Kylin][kylin]. The CRoaring library is used in several systems such as [Apache Doris](http://doris.incubator.apache.org), [ClickHouse](https://github.com/ClickHouse/ClickHouse), [Redpanda](https://github.com/redpanda-data/redpanda), and [StarRocks](https://github.com/StarRocks/starrocks). The YouTube SQL Engine, [Google Procella](https://research.google/pubs/pub48388/), uses Roaring bitmaps for indexing.

We published a peer-reviewed article on the design and evaluation of this library:

- Roaring Bitmaps: Implementation of an Optimized Software Library, Software: Practice and Experience 48 (4), 2018 [arXiv:1709.07821](https://arxiv.org/abs/1709.07821)

[lucene]: https://lucene.apache.org/
[solr]: https://lucene.apache.org/solr/
[elasticsearch]: https://www.elastic.co/products/elasticsearch
[druid]: http://druid.io/
[spark]: https://spark.apache.org/
[opensearchserver]: http://www.opensearchserver.com
[cloudtorrent]: https://github.com/jpillora/cloud-torrent
[whoosh]: https://bitbucket.org/mchaput/whoosh/wiki/Home
[pilosa]: https://www.pilosa.com/
[kylin]: http://kylin.apache.org/
[pinot]: http://github.com/linkedin/pinot/wiki
[vsts]: https://www.visualstudio.com/team-services/
[atlas]: https://github.com/Netflix/atlas

Roaring bitmaps are found to work well in many important applications:

> Use Roaring for bitmap compression whenever possible. Do not use other bitmap compression methods ([Wang et al., SIGMOD 2017](http://db.ucsd.edu/wp-content/uploads/2017/03/sidm338-wangA.pdf))


[There is a serialized format specification for interoperability between implementations](https://github.com/RoaringBitmap/RoaringFormatSpec/). Hence, it is possible to serialize a Roaring Bitmap from C++, read it in Java, modify it, serialize it back and read it in Go and Python.

# Objective

The primary goal of the CRoaring is to provide a high performance low-level implementation that fully take advantage
of the latest hardware. Roaring bitmaps are already available on a variety of platform through Java, Go, Rust... implementations. CRoaring is a library that seeks to achieve superior performance by staying close to the latest hardware.


(c) 2016-... The CRoaring authors.



# Requirements

- Linux, macOS, FreeBSD, Windows (MSYS2 and Microsoft Visual studio).
- We test the library with ARM, x64/x86 and POWER processors. We only support little endian systems (big endian systems are vanishingly rare).
- Recent C compiler supporting the C11 standard (GCC 7 or better, LLVM 8 or better (clang), Xcode 11 or better, Microsoft Visual Studio 2022 or better, Intel oneAPI Compiler 2023.2 or better), there is also an optional C++ class that requires a C++ compiler supporting the C++11 standard.
- CMake (to contribute to the project, users can rely on amalgamation/unity builds if they do not wish to use CMake).
- The CMake system assumes that git is available.
- Under x64 systems, the library provides runtime dispatch so that optimized functions are called based on the detected CPU features. It works with GCC, clang (version 9 and up) and Visual Studio (2017 and up). Other systems (e.g., ARM) do not need runtime dispatch.

Hardly anyone has access to an actual big-endian system. Nevertheless,
We support big-endian systems such as IBM s390x through emulators---except for
IO serialization which is only supported on little-endian systems (see [issue 423](https://github.com/RoaringBitmap/CRoaring/issues/423)).


# Quick Start

The CRoaring library can be amalgamated into a single source file that makes it easier
for integration into other projects. Moreover, by making it possible to compile
all the critical code into one compilation unit, it can improve the performance. For
the rationale, please see the [SQLite documentation](https://www.sqlite.org/amalgamation.html),
or the corresponding [Wikipedia entry](https://en.wikipedia.org/wiki/Single_Compilation_Unit).
Users who choose this route, do not need to rely on CRoaring's build system (based on CMake).

We offer amalgamated files as part of each release.

Linux or macOS users might follow the following instructions if they have a recent C or C++ compiler installed and a standard utility (`wget`).


 1. Pull the library in a directory
    ```
    wget https://github.com/RoaringBitmap/CRoaring/releases/download/v2.1.0/roaring.c
    wget https://github.com/RoaringBitmap/CRoaring/releases/download/v2.1.0/roaring.h
    wget https://github.com/RoaringBitmap/CRoaring/releases/download/v2.1.0/roaring.hh
    ```
 2. Create a new file named `demo.c` with this content:
    ```C
    #include <stdio.h>
    #include <stdlib.h>
    #include "roaring.c"
    int main() {
        roaring_bitmap_t *r1 = roaring_bitmap_create();
        for (uint32_t i = 100; i < 1000; i++) roaring_bitmap_add(r1, i);
        printf("cardinality = %d\n", (int) roaring_bitmap_get_cardinality(r1));
        roaring_bitmap_free(r1);

        bitset_t *b = bitset_create();
        for (int k = 0; k < 1000; ++k) {
                bitset_set(b, 3 * k);
        }
        printf("%zu \n", bitset_count(b));
        bitset_free(b);
        return EXIT_SUCCESS;
    }
    ```
 2. Create a new file named `demo.cpp` with this content:
    ```C++
    #include <iostream>
    #include "roaring.hh" // the amalgamated roaring.hh includes roaring64map.hh
    #include "roaring.c"
    int main() {
        roaring::Roaring r1;
        for (uint32_t i = 100; i < 1000; i++) {
            r1.add(i);
        }
        std::cout << "cardinality = " << r1.cardinality() << std::endl;

        roaring::Roaring64Map r2;
        for (uint64_t i = 18000000000000000100ull; i < 18000000000000001000ull; i++) {
            r2.add(i);
        }
        std::cout << "cardinality = " << r2.cardinality() << std::endl;
        return 0;
    }
    ```
 2. Compile
    ```
    cc -o demo demo.c
    c++ -std=c++11 -o demopp demo.cpp
    ```
 3. `./demo`
    ```
    cardinality = 900
    1000
    ```
 4. `./demopp`
    ```
    cardinality = 900
    cardinality = 900
    ```


# Using Roaring as a CPM dependency


If you like CMake and CPM, you can add just a few lines in your `CMakeLists.txt` file to grab a `CRoaring` release. [See our CPM demonstration for further details](https://github.com/RoaringBitmap/CPMdemo).



```CMake
cmake_minimum_required(VERSION 3.10)
project(roaring_demo
  LANGUAGES CXX C
)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 11)

add_executable(hello hello.cpp)
# You can add CPM.cmake like so:
# mkdir -p cmake
# wget -O cmake/CPM.cmake https://github.com/cpm-cmake/CPM.cmake/releases/latest/download/get_cpm.cmake
include(cmake/CPM.cmake)
CPMAddPackage(
  NAME roaring
  GITHUB_REPOSITORY "RoaringBitmap/CRoaring"
  GIT_TAG v2.0.4
  OPTIONS "BUILD_TESTING OFF"
)

target_link_libraries(hello roaring::roaring)
```


# Using as a CMake dependency with FetchContent

If you like CMake, you can add just a few lines in your `CMakeLists.txt` file to grab a `CRoaring` release. [See our demonstration for further details](https://github.com/RoaringBitmap/croaring_cmake_demo_single_file).

If you installed the CRoaring library locally, you may use it with CMake's `find_package` function as in this example:

```CMake
cmake_minimum_required(VERSION 3.15)

project(test_roaring_install VERSION 0.1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(roaring REQUIRED)

file(WRITE main.cpp "
#include <iostream>
#include \"roaring/roaring.hh\"
int main() {
  roaring::Roaring r1;
  for (uint32_t i = 100; i < 1000; i++) {
    r1.add(i);
  }
  std::cout << \"cardinality = \" << r1.cardinality() << std::endl;
  return 0;
}")

add_executable(repro main.cpp)
target_link_libraries(repro PUBLIC roaring::roaring)
```


# Amalgamating

To generate the amalgamated files yourself, you can invoke a bash script...

```bash
./amalgamation.sh
```

If you prefer a silent output, you can use the following command to redirect ``stdout`` :

```bash
./amalgamation.sh > /dev/null
```


(Bash shells are standard under Linux and macOS. Bash shells are available under Windows as part of the  [GitHub Desktop](https://desktop.github.com/) under the name ``Git Shell``. So if you have cloned the ``CRoaring`` GitHub repository from within the GitHub Desktop, you can right-click on ``CRoaring``, select ``Git Shell`` and then enter the above commands.)

It is not necessary to invoke the script in the CRoaring directory. You can invoke
it from any directory where you want the amalgamation files to be written.

It will generate three files for C users: ``roaring.h``, ``roaring.c`` and ``amalgamation_demo.c``... as well as some brief instructions. The ``amalgamation_demo.c`` file is a short example, whereas ``roaring.h`` and ``roaring.c`` are "amalgamated" files (including all source and header files for the project). This means that you can simply copy the files ``roaring.h`` and ``roaring.c`` into your project and be ready to go! No need to produce a library! See the ``amalgamation_demo.c`` file.

# API

The C interface is found in the files

- [roaring.h](https://github.com/RoaringBitmap/CRoaring/blob/master/include/roaring/roaring.h),
- [roaring64.h](https://github.com/RoaringBitmap/CRoaring/blob/master/include/roaring/roaring64.h).

We also have a C++ interface:

- [roaring.hh](https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring.hh),
- [roaring64map.hh](https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh).


# Dealing with large volumes

Some users have to deal with large volumes of data. It  may be important for these users to be aware of the `addMany` (C++) `roaring_bitmap_or_many` (C) functions as it is much faster and economical to add values in batches when possible. Furthermore, calling periodically the `runOptimize` (C++) or `roaring_bitmap_run_optimize` (C) functions may help.


# Running microbenchmarks

We have microbenchmarks constructed with the Google Benchmarks.
Under Linux or macOS, you may run them as follows:

```
cmake -B build -D ENABLE_ROARING_MICROBENCHMARKS=ON
cmake --build build
./build/microbenchmarks/bench
```

By default, the benchmark tools picks one data set (e.g., `CRoaring/benchmarks/realdata/census1881`).
We have several data sets and you may pick others:

```
./build/microbenchmarks/bench benchmarks/realdata/wikileaks-noquotes
```

You may disable some functionality for the purpose of benchmarking. For example, assuming you
have an x64 processor, you could benchmark the code without AVX-512 even if both your processor
and compiler supports it:

```
cmake -B buildnoavx512 -D ROARING_DISABLE_AVX512=ON -D ENABLE_ROARING_MICROBENCHMARKS=ON
cmake --build buildnoavx512
./buildnoavx512/microbenchmarks/bench
```

You can benchmark without AVX or AVX-512 as well:

```
cmake -B buildnoavx -D ROARING_DISABLE_AVX=ON -D ENABLE_ROARING_MICROBENCHMARKS=ON
cmake --build buildnoavx
./buildnoavx/microbenchmarks/bench
```

# Custom memory allocators
For general users, CRoaring would apply default allocator without extra codes. But global memory hook is also provided for those who want a custom memory allocator. Here is an example:
```C
#include <roaring.h>

int main(){
    // define with your own memory hook
    roaring_memory_t my_hook{my_malloc, my_free ...};
    // initialize global memory hook
    roaring_init_memory_hook(my_hook);
    // write you code here
    ...
}
```


# Example (C)


This example assumes that CRoaring has been build and that you are linking against the corresponding library. By default, CRoaring will install its header files in a `roaring` directory. If you are working from the amalgamation script, you may add the line `#include "roaring.c"` if you are not linking against a prebuilt CRoaring library and replace `#include <roaring/roaring.h>` by `#include "roaring.h"`.

```c
#include <roaring/roaring.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

bool roaring_iterator_sumall(uint32_t value, void *param) {
    *(uint32_t *)param += value;
    return true;  // iterate till the end
}

int main() {
    // create a new empty bitmap
    roaring_bitmap_t *r1 = roaring_bitmap_create();
    // then we can add values
    for (uint32_t i = 100; i < 1000; i++) roaring_bitmap_add(r1, i);
    // check whether a value is contained
    assert(roaring_bitmap_contains(r1, 500));
    // compute how many bits there are:
    uint32_t cardinality = roaring_bitmap_get_cardinality(r1);
    printf("Cardinality = %d \n", cardinality);

    // if your bitmaps have long runs, you can compress them by calling
    // run_optimize
    uint32_t expectedsizebasic = roaring_bitmap_portable_size_in_bytes(r1);
    roaring_bitmap_run_optimize(r1);
    uint32_t expectedsizerun = roaring_bitmap_portable_size_in_bytes(r1);
    printf("size before run optimize %d bytes, and after %d bytes\n",
           expectedsizebasic, expectedsizerun);

    // create a new bitmap containing the values {1,2,3,5,6}
    roaring_bitmap_t *r2 = roaring_bitmap_from(1, 2, 3, 5, 6);
    roaring_bitmap_printf(r2);  // print it

    // we can also create a bitmap from a pointer to 32-bit integers
    uint32_t somevalues[] = {2, 3, 4};
    roaring_bitmap_t *r3 = roaring_bitmap_of_ptr(3, somevalues);

    // we can also go in reverse and go from arrays to bitmaps
    uint64_t card1 = roaring_bitmap_get_cardinality(r1);
    uint32_t *arr1 = (uint32_t *)malloc(card1 * sizeof(uint32_t));
    assert(arr1 != NULL);
    roaring_bitmap_to_uint32_array(r1, arr1);
    roaring_bitmap_t *r1f = roaring_bitmap_of_ptr(card1, arr1);
    free(arr1);
    assert(roaring_bitmap_equals(r1, r1f));  // what we recover is equal
    roaring_bitmap_free(r1f);

    // we can go from arrays to bitmaps from "offset" by "limit"
    size_t offset = 100;
    size_t limit = 1000;
    uint32_t *arr3 = (uint32_t *)malloc(limit * sizeof(uint32_t));
    assert(arr3 != NULL);
    roaring_bitmap_range_uint32_array(r1, offset, limit, arr3);
    free(arr3);

    // we can copy and compare bitmaps
    roaring_bitmap_t *z = roaring_bitmap_copy(r3);
    assert(roaring_bitmap_equals(r3, z));  // what we recover is equal
    roaring_bitmap_free(z);

    // we can compute union two-by-two
    roaring_bitmap_t *r1_2_3 = roaring_bitmap_or(r1, r2);
    roaring_bitmap_or_inplace(r1_2_3, r3);

    // we can compute a big union
    const roaring_bitmap_t *allmybitmaps[] = {r1, r2, r3};
    roaring_bitmap_t *bigunion = roaring_bitmap_or_many(3, allmybitmaps);
    assert(
        roaring_bitmap_equals(r1_2_3, bigunion));  // what we recover is equal
    // can also do the big union with a heap
    roaring_bitmap_t *bigunionheap =
        roaring_bitmap_or_many_heap(3, allmybitmaps);
    assert(roaring_bitmap_equals(r1_2_3, bigunionheap));

    roaring_bitmap_free(r1_2_3);
    roaring_bitmap_free(bigunion);
    roaring_bitmap_free(bigunionheap);

    // we can compute intersection two-by-two
    roaring_bitmap_t *i1_2 = roaring_bitmap_and(r1, r2);
    roaring_bitmap_free(i1_2);

    // we can write a bitmap to a pointer and recover it later
    uint32_t expectedsize = roaring_bitmap_portable_size_in_bytes(r1);
    char *serializedbytes = malloc(expectedsize);
    roaring_bitmap_portable_serialize(r1, serializedbytes);
    // Note: it is expected that the input follows the specification
    // https://github.com/RoaringBitmap/RoaringFormatSpec
    // otherwise the result may be unusable.
    roaring_bitmap_t *t = roaring_bitmap_portable_deserialize_safe(serializedbytes, expectedsize);
    if(t == NULL) { return EXIT_FAILURE; }
    const char *reason = NULL;
    if (!roaring_bitmap_internal_validate(t, &reason)) {
        return EXIT_FAILURE;
    }
    assert(roaring_bitmap_equals(r1, t));  // what we recover is equal
    roaring_bitmap_free(t);
    // we can also check whether there is a bitmap at a memory location without
    // reading it
    size_t sizeofbitmap =
        roaring_bitmap_portable_deserialize_size(serializedbytes, expectedsize);
    assert(sizeofbitmap ==
           expectedsize);  // sizeofbitmap would be zero if no bitmap were found
    // we can also read the bitmap "safely" by specifying a byte size limit:
    t = roaring_bitmap_portable_deserialize_safe(serializedbytes, expectedsize);
    if(t == NULL) {
        printf("Problem during deserialization.\n");
        // We could clear any memory and close any file here.
        return EXIT_FAILURE;
    }
    // We can validate the bitmap we recovered to make sure it is proper.
    const char *reason_failure = NULL;
    if (!roaring_bitmap_internal_validate(t, &reason_failure)) {
        printf("safely deserialized invalid bitmap: %s\n", reason_failure);
        // We could clear any memory and close any file here.
        return EXIT_FAILURE;
    }
    // It is still necessary for the content of seriallizedbytes to follow
    // the standard: https://github.com/RoaringBitmap/RoaringFormatSpec
    // This is guaranted when calling 'roaring_bitmap_portable_deserialize'.
    assert(roaring_bitmap_equals(r1, t));  // what we recover is equal
    roaring_bitmap_free(t);

    free(serializedbytes);

    // we can iterate over all values using custom functions
    uint32_t counter = 0;
    roaring_iterate(r1, roaring_iterator_sumall, &counter);

    // we can also create iterator structs
    counter = 0;
    roaring_uint32_iterator_t *i = roaring_iterator_create(r1);
    while (i->has_value) {
        counter++;  // could use    i->current_value
        roaring_uint32_iterator_advance(i);
    }
    // you can skip over values and move the iterator with
    // roaring_uint32_iterator_move_equalorlarger(i,someintvalue)

    roaring_uint32_iterator_free(i);
    // roaring_bitmap_get_cardinality(r1) == counter

    // for greater speed, you can iterate over the data in bulk
    i = roaring_iterator_create(r1);
    uint32_t buffer[256];
    while (1) {
        uint32_t ret = roaring_uint32_iterator_read(i, buffer, 256);
        for (uint32_t j = 0; j < ret; j++) {
            counter += buffer[j];
        }
        if (ret < 256) {
            break;
        }
    }
    roaring_uint32_iterator_free(i);

    roaring_bitmap_free(r1);
    roaring_bitmap_free(r2);
    roaring_bitmap_free(r3);
    return EXIT_SUCCESS;
}
```

# Compressed 64-bit Roaring bitmaps (C)


We also support efficient 64-bit compressed bitmaps in C:

```c++
  roaring64_bitmap_t *r2 = roaring64_bitmap_create();
  for (uint64_t i = 100; i < 1000; i++) roaring64_bitmap_add(r2, i);
  printf("cardinality (64-bit) = %d\n", (int) roaring64_bitmap_get_cardinality(r2));
  roaring64_bitmap_free(r2);
```


# Conventional bitsets (C)

We support convention bitsets (uncompressed) as part of the library.

Simple example:

```C
bitset_t * b = bitset_create();
bitset_set(b,10);
bitset_get(b,10);// returns true
bitset_free(b); // frees memory
```

More advanced example:

```C
    bitset_t *b = bitset_create();
    for (int k = 0; k < 1000; ++k) {
        bitset_set(b, 3 * k);
    }
    // We have bitset_count(b) == 1000.
    // We have bitset_get(b, 3) is true
    // You can iterate through the values:
    size_t k = 0;
    for (size_t i = 0; bitset_next_set_bit(b, &i); i++) {
        // You will have i == k
        k += 3;
    }
    // We support a wide range of operations on two bitsets such as
    // bitset_inplace_symmetric_difference(b1,b2);
    // bitset_inplace_symmetric_difference(b1,b2);
    // bitset_inplace_difference(b1,b2);// should make no difference
    // bitset_inplace_union(b1,b2);
    // bitset_inplace_intersection(b1,b2);
    // bitsets_disjoint
    // bitsets_intersect
```

In some instances, you may want to convert a Roaring bitmap into a conventional (uncompressed) bitset.
Indeed, bitsets have advantages such as higher query performances in some cases. The following code
illustrates how you may do so:

```C
    roaring_bitmap_t *r1 = roaring_bitmap_create();
    for (uint32_t i = 100; i < 100000; i+= 1 + (i%5)) {
     roaring_bitmap_add(r1, i);
    }
    for (uint32_t i = 100000; i < 500000; i+= 100) {
     roaring_bitmap_add(r1, i);
    }
    roaring_bitmap_add_range(r1, 500000, 600000);
    bitset_t * bitset = bitset_create();
    bool success = roaring_bitmap_to_bitset(r1, bitset);
    assert(success); // could fail due to memory allocation.
    assert(bitset_count(bitset) == roaring_bitmap_get_cardinality(r1));
    // You can then query the bitset:
    for (uint32_t i = 100; i < 100000; i+= 1 + (i%5)) {
        assert(bitset_get(bitset,i));
    }
    for (uint32_t i = 100000; i < 500000; i+= 100) {
        assert(bitset_get(bitset,i));
    }
    // you must free the memory:
    bitset_free(bitset);
    roaring_bitmap_free(r1);
```

You should be aware that a convention bitset (`bitset_t *`) may use much more
memory than a Roaring bitmap in some cases. You should run benchmarks to determine
whether the conversion to a bitset has performance benefits in your case.

# Example (C++)


This example assumes that CRoaring has been build and that you are linking against the corresponding library. By default, CRoaring will install its header files in a `roaring` directory so you may need to replace `#include "roaring.hh"` by `#include <roaring/roaring.hh>`. If you are working from the amalgamation script, you may add the line `#include "roaring.c"` if you are not linking against a CRoaring prebuilt library.

```c++
#include <iostream>

#include "roaring.hh"

using namespace roaring;

int main() {
    Roaring r1;
    for (uint32_t i = 100; i < 1000; i++) {
        r1.add(i);
    }

    // check whether a value is contained
    assert(r1.contains(500));

    // compute how many bits there are:
    uint32_t cardinality = r1.cardinality();

    // if your bitmaps have long runs, you can compress them by calling
    // run_optimize
    uint32_t size = r1.getSizeInBytes();
    r1.runOptimize();

    // you can enable "copy-on-write" for fast and shallow copies
    r1.setCopyOnWrite(true);

    uint32_t compact_size = r1.getSizeInBytes();
    std::cout << "size before run optimize " << size << " bytes, and after "
              << compact_size << " bytes." << std::endl;

    // create a new bitmap with varargs
    Roaring r2 = Roaring::bitmapOf(5, 1, 2, 3, 5, 6);

    r2.printf();
    printf("\n");

    // create a new bitmap with initializer list
    Roaring r2i = Roaring::bitmapOfList({1, 2, 3, 5, 6});

    assert(r2i == r2);

    // we can also create a bitmap from a pointer to 32-bit integers
    const uint32_t values[] = {2, 3, 4};
    Roaring r3(3, values);

    // we can also go in reverse and go from arrays to bitmaps
    uint64_t card1 = r1.cardinality();
    uint32_t *arr1 = new uint32_t[card1];
    r1.toUint32Array(arr1);
    Roaring r1f(card1, arr1);
    delete[] arr1;

    // bitmaps shall be equal
    assert(r1 == r1f);

    // we can copy and compare bitmaps
    Roaring z(r3);
    assert(r3 == z);

    // we can compute union two-by-two
    Roaring r1_2_3 = r1 | r2;
    r1_2_3 |= r3;

    // we can compute a big union
    const Roaring *allmybitmaps[] = {&r1, &r2, &r3};
    Roaring bigunion = Roaring::fastunion(3, allmybitmaps);
    assert(r1_2_3 == bigunion);

    // we can compute intersection two-by-two
    Roaring i1_2 = r1 & r2;

    // we can write a bitmap to a pointer and recover it later
    uint32_t expectedsize = r1.getSizeInBytes();
    char *serializedbytes = new char[expectedsize];
    r1.write(serializedbytes);
    // readSafe will not overflow, but the resulting bitmap
    // is only valid and usable if the input follows the
    // Roaring specification: https://github.com/RoaringBitmap/RoaringFormatSpec/
    Roaring t = Roaring::readSafe(serializedbytes, expectedsize);
    assert(r1 == t);
    delete[] serializedbytes;

    // we can iterate over all values using custom functions
    uint32_t counter = 0;
    r1.iterate(
        [](uint32_t value, void *param) {
            *(uint32_t *)param += value;
            return true;
        },
        &counter);

    // we can also iterate the C++ way
    counter = 0;
    for (Roaring::const_iterator i = t.begin(); i != t.end(); i++) {
        ++counter;
    }
    // counter == t.cardinality()

    // we can move iterators to skip values
    const uint32_t manyvalues[] = {2, 3, 4, 7, 8};
    Roaring rogue(5, manyvalues);
    Roaring::const_iterator j = rogue.begin();
    j.equalorlarger(4);  // *j == 4
    return EXIT_SUCCESS;
}

```



# Building with cmake (Linux and macOS, Visual Studio users should see below)

CRoaring follows the standard cmake workflow. Starting from the root directory of
the project (CRoaring), you can do:

```
mkdir -p build
cd build
cmake ..
cmake --build .
# follow by 'ctest' if you want to test.
# you can also type 'make install' to install the library on your system
# C header files typically get installed to /usr/local/include/roaring
# whereas C++ header files get installed to /usr/local/include/roaring
```
(You can replace the ``build`` directory with any other directory name.)
By default all tests are built on all platforms, to skip building and running tests add `` -DENABLE_ROARING_TESTS=OFF `` to the command line.

As with all ``cmake`` projects, you can specify the compilers you wish to use by adding (for example) ``-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++`` to the ``cmake`` command line.

If you are using clang or gcc and you know your target architecture,  you can set the architecture by specifying `-DROARING_ARCH=arch`. For example, if you have many server but the oldest server is running the Intel `haswell` architecture, you can specify -`DROARING_ARCH=haswell`. In such cases, the produced binary will be optimized for processors having the characteristics of a haswell process and may not run on older architectures. You can find out the list of valid architecture values by typing `man gcc`.

 ```
 mkdir -p build_haswell
 cd build_haswell
 cmake -DROARING_ARCH=haswell ..
 cmake --build .
 ```

For a debug release, starting from the root directory of the project (CRoaring), try

```
mkdir -p debug
cd debug
cmake -DCMAKE_BUILD_TYPE=Debug -DROARING_SANITIZE=ON ..
ctest
```


To check that your code abides by the style convention (make sure that ``clang-format`` is installed):

```
./tools/clang-format-check.sh
```

To reformat your code according to the style convention (make sure that ``clang-format`` is installed):

```
./tools/clang-format.sh
```

# Building (Visual Studio under Windows)

We are assuming that you have a common Windows PC with at least Visual Studio 2015, and an x64 processor.

To build with at least Visual Studio 2015 from the command line:
- Grab the CRoaring code from GitHub, e.g., by cloning it using [GitHub Desktop](https://desktop.github.com/).
- Install [CMake](https://cmake.org/download/). When you install it, make sure to ask that ``cmake`` be made available from the command line.
- Create a subdirectory within CRoaring, such as ``VisualStudio``.
- Using a shell, go to this newly created directory. For example, within GitHub Desktop, you can right-click on  ``CRoaring`` in your GitHub repository list, and select ``Open in Git Shell``, then type ``cd VisualStudio`` in the newly created shell.
- Type ``cmake -DCMAKE_GENERATOR_PLATFORM=x64 ..`` in the shell while in the ``VisualStudio`` repository. (Alternatively, if you want to build a static library, you may use the command line ``cmake -DCMAKE_GENERATOR_PLATFORM=x64 -DROARING_BUILD_STATIC=ON  ..``.)
- This last command created a Visual Studio solution file in the newly created directory (e.g., ``RoaringBitmap.sln``). Open this file in Visual Studio. You should now be able to build the project and run the tests. For example, in the ``Solution Explorer`` window (available from the ``View`` menu), right-click ``ALL_BUILD`` and select ``Build``. To test the code, still in the ``Solution Explorer`` window, select ``RUN_TESTS`` and select ``Build``.

To build with at least Visual Studio 2017 directly in the IDE:
- Grab the CRoaring code from GitHub, e.g., by cloning it using [GitHub Desktop](https://desktop.github.com/).
- Select the ``Visual C++ tools for CMake`` optional component when installing the C++ Development Workload within Visual Studio.
- Within Visual Studio use ``File > Open > Folder...`` to open the CRoaring folder.
- Right click on ``CMakeLists.txt`` in the parent directory within ``Solution Explorer`` and select ``Build`` to build the project.
- For testing, in the Standard toolbar, drop the ``Select Startup Item...`` menu and choose one of the tests. Run the test by pressing the button to the left of the dropdown.


We have optimizations specific to AVX2 and AVX-512 in the code, and they are turned dynamically based on the detected hardware at runtime.


## Usage (Using `conan`)

You can install the library using the conan package manager:

```
$ echo -e "[requires]\nroaring/0.3.3" > conanfile.txt
$ conan install .
```


## Usage (Using `vcpkg` on Windows, Linux and macOS)

[vcpkg](https://github.com/Microsoft/vcpkg) users on Windows, Linux and macOS can download and install `roaring` with one single command from their favorite shell.

On Linux and macOS:

```
$ ./vcpkg install roaring
```

will build and install `roaring` as a static library.

On Windows (64-bit):

```
.\vcpkg.exe install roaring:x64-windows
```

will build and install `roaring` as a shared library.

```
.\vcpkg.exe install roaring:x64-windows-static
```

will build and install `roaring` as a static library.

These commands will also print out instructions on how to use the library from MSBuild or CMake-based projects.

If you find the version of `roaring` shipped with `vcpkg` is out-of-date, feel free to report it to `vcpkg` community either by submiting an issue or by creating a PR.

# SIMD-related throttling

Our AVX2 code does not use floating-point numbers or multiplications, so it is not subject to turbo frequency throttling on many-core Intel processors.

Our AVX-512 code is only enabled on recent hardware (Intel Ice Lake or better and AMD Zen 4) where SIMD-specific frequency throttling is not observed.

# Thread safety

Like, for example, STL containers, the CRoaring library has no built-in thread support. Thus whenever you modify a bitmap in one thread, it is unsafe to query it in others. However, you can safely copy a bitmap and use both copies in concurrently.

If you use  "copy-on-write" (default to disabled), then you should pass copies to the different threads. They will create shared containers, and for shared containers, we use reference counting with an atomic counter.



To summarize:
- If you do not use copy-on-write, you can access concurrent the same bitmap safely as long as you do not modify it. If you plan on modifying it, you should pass different copies to the different threads.
- If you use copy-on-write, you should always pass copies to the different threads. The copies and then lightweight (shared containers).

Thus the following pattern where you copy bitmaps and pass them to different threads is safe with or without COW:

```C
    roaring_bitmap_set_copy_on_write(r1, true);
    roaring_bitmap_set_copy_on_write(r2, true);
    roaring_bitmap_set_copy_on_write(r3, true);

    roaring_bitmap_t * r1a = roaring_bitmap_copy(r1);
    roaring_bitmap_t * r1b = roaring_bitmap_copy(r1);

    roaring_bitmap_t * r2a = roaring_bitmap_copy(r2);
    roaring_bitmap_t * r2b = roaring_bitmap_copy(r2);

    roaring_bitmap_t * r3a = roaring_bitmap_copy(r3);
    roaring_bitmap_t * r3b = roaring_bitmap_copy(r3);

    roaring_bitmap_t *rarray1[3] = {r1a, r2a, r3a};
    roaring_bitmap_t *rarray2[3] = {r1b, r2b, r3b};
    std::thread thread1(run, rarray1);
    std::thread thread2(run, rarray2);
```

# How to best aggregate bitmaps?

Suppose you want to compute the union (OR) of many bitmaps. How do you proceed? There are many
different strategies.

You can use `roaring_bitmap_or_many(bitmapcount, bitmaps)` or `roaring_bitmap_or_many_heap(bitmapcount, bitmaps)` or you may
even roll your own aggregation:

```C
roaring_bitmap_t *answer = roaring_bitmap_copy(bitmaps[0]);
for (size_t i = 1; i < bitmapcount; i++) {
  roaring_bitmap_or_inplace(answer, bitmaps[i]);
}
```

All of them will work but they have different performance characteristics. The `roaring_bitmap_or_many_heap` should
probably only be used if, after benchmarking, you find that it is faster by a good margin: it uses more memory.

The `roaring_bitmap_or_many` is meant as a good default. It works by trying to delay work as much as possible.
However, because it delays computations, it also does not optimize the format as the computation runs. It might
thus fail to see some useful pattern in the data such as long consecutive values.

The approach based on repeated calls to `roaring_bitmap_or_inplace`
is also fine, and might even be faster in some cases. You can expect it to be faster if, after
a few calls, you get long sequences of consecutive values in the answer. That is, if the
final answer is all integers in the range [0,1000000), and this is apparent quickly, then the
later `roaring_bitmap_or_inplace` will be very fast.

You should benchmark these alternatives on your own data to decide what is best.

# Wrappers

## Python
Tom Cornebize wrote a Python wrapper available at https://github.com/Ezibenroc/PyRoaringBitMap
Installing it is as easy as typing...

```
pip install pyroaring
```

## JavaScript

Salvatore Previti  wrote a Node/JavaScript wrapper available at https://github.com/SalvatorePreviti/roaring-node
Installing it is as easy as typing...

```
npm install roaring
```

## Swift

Jérémie Piotte wrote a [Swift wrapper](https://github.com/RoaringBitmap/SwiftRoaring).


## C#

Brandon Smith wrote a C# wrapper available at https://github.com/RogueException/CRoaring.Net (works for Windows and Linux under x64 processors)


## Go (golang)

There is a Go (golang) wrapper available at https://github.com/RoaringBitmap/gocroaring

## Rust

Saulius Grigaliunas wrote a Rust wrapper available at https://github.com/saulius/croaring-rs

## D

Yuce Tekol wrote a D wrapper available at https://github.com/yuce/droaring

## Redis

Antonio Guilherme Ferreira Viggiano wrote a Redis Module available at https://github.com/aviggiano/redis-roaring

## Zig

Justin Whear wrote a Zig wrapper available at https://github.com/jwhear/roaring-zig


# Mailing list/discussion group

https://groups.google.com/forum/#!forum/roaring-bitmaps

# Contributing

When contributing a change to the project, please run `tools/clang-format.sh` after making any changes. A github action runs on all PRs to ensure formatting is consistent with this.

# References about Roaring

- Daniel Lemire, Owen Kaser, Nathan Kurz, Luca Deri, Chris O'Hara, François Saint-Jacques, Gregory Ssi-Yan-Kai, Roaring Bitmaps: Implementation of an Optimized Software Library, Software: Practice and Experience Volume 48, Issue 4 April 2018 Pages 867-895 [arXiv:1709.07821](https://arxiv.org/abs/1709.07821)
-  Samy Chambi, Daniel Lemire, Owen Kaser, Robert Godin,
Better bitmap performance with Roaring bitmaps,
Software: Practice and Experience Volume 46, Issue 5, pages 709–719, May 2016  [arXiv:1402.6407](http://arxiv.org/abs/1402.6407)
- Daniel Lemire, Gregory Ssi-Yan-Kai, Owen Kaser, Consistently faster and smaller compressed bitmaps with Roaring, Software: Practice and Experience Volume 46, Issue 11, pages 1547-1569, November 2016 [arXiv:1603.06549](http://arxiv.org/abs/1603.06549)
- Samy Chambi, Daniel Lemire, Robert Godin, Kamel Boukhalfa, Charles Allen, Fangjin Yang, Optimizing Druid with Roaring bitmaps, IDEAS 2016, 2016. http://r-libre.teluq.ca/950/
