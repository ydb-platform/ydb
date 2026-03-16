# Blosc: A blocking, shuffling and lossless compression library
| Author | Contact | URL |
|--------|---------|-----|
| Blosc Development Team | blosc@blosc.org | https://www.blosc.org | 

| Gitter | GH Actions | NumFOCUS | Code of Conduct |
|--------|------------|----------|-----------------|
| [![Gitter](https://badges.gitter.im/Blosc/c-blosc.svg)](https://gitter.im/Blosc/c-blosc?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) | [![CI CMake](https://github.com/Blosc/c-blosc/workflows/CI%20CMake/badge.svg)](https://github.com/Blosc/c-blosc/actions?query=workflow%3A%22CI+CMake%22) | [![Powered by NumFOCUS](https://img.shields.io/badge/powered%20by-NumFOCUS-orange.svg?style=flat&colorA=E1523D&colorB=007D8A)](https://numfocus.org) | [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](code_of_conduct.md) |

## What is it?

**Note**: There is a more modern version of this package called C-Blosc2
which supports many more features and is more actively maintained.  Visit it at:
https://github.com/Blosc/c-blosc2

Blosc is a high performance compressor optimized for binary data.
It has been designed to transmit data to the processor cache faster
than the traditional, non-compressed, direct memory fetch approach via
a memcpy() OS call.  Blosc is the first compressor (that I'm aware of)
that is meant not only to reduce the size of large datasets on-disk or
in-memory, but also to accelerate memory-bound computations.

It uses the [blocking technique](https://www.blosc.org/docs/StarvingCPUs-CISE-2010.pdf)
so as to reduce activity in the memory bus as much as possible. In short, this
technique works by dividing datasets in blocks that are small enough
to fit in caches of modern processors and perform compression /
decompression there.  It also leverages, if available, SIMD
instructions (SSE2, AVX2) and multi-threading capabilities of CPUs, in
order to accelerate the compression / decompression process to a
maximum.

See some [benchmarks](https://www.blosc.org/pages/synthetic-benchmarks/) about Blosc performance.

Blosc is distributed using the BSD license, see LICENSE.txt for
details.

## Meta-compression and other differences over existing compressors

C-Blosc is not like other compressors: it should rather be called a
meta-compressor.  This is so because it can use different compressors
and filters (programs that generally improve compression ratio).  At
any rate, it can also be called a compressor because it happens that
it already comes with several compressor and filters, so it can
actually work like a regular codec.

Currently C-Blosc comes with support of BloscLZ, a compressor heavily
based on FastLZ (https://ariya.github.io/FastLZ/), LZ4 and LZ4HC
(https://lz4.org/), Snappy
(https://google.github.io/snappy/), Zlib (https://zlib.net/) and
Zstandard (https://facebook.github.io/zstd/).

C-Blosc also comes with highly optimized (they can use
SSE2 or AVX2 instructions, if available) shuffle and bitshuffle filters
(for info on how and why shuffling works [see here](https://www.slideshare.net/PyData/blosc-py-data-2014/17?src=clipshare)).
However, additional compressors or filters may be added in the future.

Blosc is in charge of coordinating the different compressor and
filters so that they can leverage the 
[blocking technique](https://www.blosc.org/docs/StarvingCPUs-CISE-2010.pdf)
as well as multi-threaded execution (if several cores are
available) automatically. That makes that every codec and filter
will work at very high speeds, even if it was not initially designed
for doing blocking or multi-threading.

Finally, C-Blosc is specially suited to deal with binary data because
it can take advantage of the type size meta-information for improved
compression ratio by using the integrated shuffle and bitshuffle filters.

When taken together, all these features set Blosc apart from other
compression libraries.

## Compiling the Blosc library

Blosc can be built, tested and installed using CMake_.
The following procedure describes the "out of source" build.

```console

  $ cd c-blosc
  $ mkdir build
  $ cd build
```

Now run CMake configuration and optionally specify the installation
directory (e.g. '/usr' or '/usr/local'):

```console

  $ cmake -DCMAKE_INSTALL_PREFIX=your_install_prefix_directory ..
```

CMake allows to configure Blosc in many different ways, like preferring
internal or external sources for compressors or enabling/disabling
them.  Please note that configuration can also be performed using UI
tools provided by [CMake](https://cmake.org) (ccmake or cmake-gui):

```console

  $ ccmake ..      # run a curses-based interface
  $ cmake-gui ..   # run a graphical interface
```

Build, test and install Blosc:


```console

  $ cmake --build .
  $ ctest
  $ cmake --build . --target install
```

The static and dynamic version of the Blosc library, together with
header files, will be installed into the specified
CMAKE_INSTALL_PREFIX.

### Codec support with CMake

C-Blosc comes with full sources for LZ4, LZ4HC, Snappy, Zlib and Zstd
and in general, you should not worry about not having (or CMake
not finding) the libraries in your system because by default the
included sources will be automatically compiled and included in the
C-Blosc library. This effectively means that you can be confident in
having a complete support for all the codecs in all the Blosc deployments
(unless you are explicitly excluding support for some of them).

But in case you want to force Blosc to use external codec libraries instead of
the included sources, you can do that:

``` console

  $ cmake -DPREFER_EXTERNAL_ZSTD=ON ..
```

You can also disable support for some compression libraries:


```console

  $ cmake -DDEACTIVATE_SNAPPY=ON ..  # in case you don't have a C++ compiler
```
 
## Examples

In the [examples/ directory](https://github.com/Blosc/c-blosc/tree/master/examples)
you can find hints on how to use Blosc inside your app.

## Supported platforms

Blosc is meant to support all platforms where a C89 compliant C
compiler can be found.  The ones that are mostly tested are Intel
(Linux, Mac OSX and Windows) and ARM (Linux), but exotic ones as IBM
Blue Gene Q embedded "A2" processor are reported to work too.

### Mac OSX troubleshooting

If you run into compilation troubles when using Mac OSX, please make
sure that you have installed the command line developer tools.  You
can always install them with:

```console

  $ xcode-select --install
```

## Wrapper for Python

Blosc has an official wrapper for Python.  See:

https://github.com/Blosc/python-blosc

## Command line interface and serialization format for Blosc

Blosc can be used from command line by using Bloscpack.  See:

https://github.com/Blosc/bloscpack

## Filter for HDF5

For those who want to use Blosc as a filter in the HDF5 library,
there is a sample implementation in the hdf5-blosc project in:

https://github.com/Blosc/hdf5-blosc

## Mailing list

There is an official mailing list for Blosc at:

blosc@googlegroups.com
https://groups.google.com/g/blosc

## Acknowledgments

See THANKS.rst.


----

  **Enjoy data!**
