===========================
 Release notes for C-Blosc
===========================

Changes from 1.21.6 to 1.21.7
=============================

* Internal LZ4 codec updated to 1.10.0.


Changes from 1.21.5 to 1.21.6
=============================

* Zlib updated to 1.3.1.  Thanks to Lachlan Deakin.
* Zstd updated to 1.5.6
* Fixed many typos.  Thanks to Dimitri Papadopoulos.


Changes from 1.21.4 to 1.21.5
=============================


* Fix SSE2/AVX2 build issue.  Fixes #352. Thanks to Thomas VINCENT
  and Mark Kittisopikul.


Changes from 1.21.3 to 1.21.4
=============================

* Upgrade internal-complib zstd from 1.5.2 to 1.5.5.

* Zlib updated to 1.2.13.


Changes from 1.21.2 to 1.21.3
=============================

* Internal LZ4 codec updated to 1.9.4.

* Internal BloscLZ codec updated to 2.5.1.


Changes from 1.21.1 to 1.21.2
=============================

* Add support for SHUFFLE_AVX2_ENABLED and SHUFFLE_SSE2_ENABLED
  even if AVX2 or SSE2 is not available.  See PR #347. Thanks to
  Thomas VINCENT.

* Upgrade internal-complib zstd from 1.5.0 to 1.5.2.  Thanks to
  Mark Kittisopikul.

* Many small code improvements, improved consistency and typo fixes.
  Thanks to Dimitri Papadopoulos Orfanos.

* New HIDE_SYMBOLS CMake option to control the symbols exposure.
  Default is ON.  Thanks to Mariusz Zaborski.


Changes from 1.21.0 to 1.21.1
=============================

* Fix pthread flag when linking on ppc64le.  See #318.  Thanks to Axel Huebl.

* Updates in codecs (some bring important performance improvements):
  * BloscLZ updated to 2.5.1
  * Zlib updated to 1.2.11
  * Zstd updated to 1.5.0


Changes from 1.20.1 to 1.21.0
=============================

* Updated zstd codec to 1.4.8.

* Updated lz4 codec to 1.9.3.

* New instructions on how to use the libraries in python-blosc wheels
  so as to compile C-Blosc applications.  See:
  https://github.com/Blosc/c-blosc/blob/master/COMPILING_WITH_WHEELS.rst


Changes from 1.20.0 to 1.20.1
=============================

* Added `<unistd.h>` in vendored zlib 1.2.8 for compatibility with Python 3.8
  in recent Mac OSX.  For details, see:
  https://github.com/Blosc/python-blosc/issues/229


Changes from 1.19.1 to 1.20.0
=============================

* More safety checks have been implemented so that potential flaws
  discovered by new fuzzers in OSS-Fuzzer are fixed now.  Thanks to
  Nathan Moinvaziri (@nmoinvaz).

* BloscLZ updated to 2.3.0. Expect better compression ratios for faster
  codecs.  For details, see our new blog post:
  https://blosc.org/posts/beast-release/

* Fixed the `_xgetbv()` collision. Thanks to Michał Górny (@mgorny).

* The chunk format has been fully described so that 3rd party software
  may come with a different implementation, but still compatible with
  C-Blosc chunks.


Changes from 1.19.0 to 1.19.1
=============================

- pthread_create() errors are now handled and propagated back to the user.
  See https://github.com/Blosc/c-blosc/pull/299.


Changes from 1.18.1 to 1.19.0
=============================

- The length of automatic blocksizes for fast codecs (lz4, blosclz) has
  been incremented quite a bit (up to 256 KB) for better compression ratios.
  The performance in modern CPUs (with at least 256 KB in L2 cache) should
  be better too (for older CPUs the performance should stay roughly the same).

- Continuous integration has been migrated to GitHub actions and much
  more scenarios are tested (specially linking with external codecs).
  Also, a new OSS-Fuzz workflow has been added for increased detection
  of possible vulnerabilities.  Thanks to Nathan Moinvaziri.

- For small buffers that cannot be compressed (typically < 128 bytes),
  `blosc_compress()` returns now a 0 (cannot compress) instead of a negative
  number (internal error).  See https://github.com/Blosc/c-blosc/pull/294.
  Thanks to @kalvdans for providing the initial patch.

- blosclz codec updated to 2.1.0.  Expect better compression ratios and
  performance in a wider variety of scenarios.

- `blosc_decompress_unsafe()`, `blosc_decompress_ctx_unsafe()` and
  `blosc_getitem_unsafe()` have been removed because they are dangerous
  and after latest improvements, they should not be used in production.

- zstd codec updated to 1.4.5.

- Conan packaging has been deprecated (from now on, we should try
  to focus on supporting wheels only).


Changes from 1.17.1 to 1.18.1
=============================

- Fixed the copy of the leftovers of a chunk when its size is not a
  multiple of the typesize.  Although this is a very unusual situation,
  it can certainly happen (e.g.
  https://github.com/Blosc/python-blosc/issues/220).


Changes from 1.17.0 to 1.17.1
=============================

- Zstd codec updated to 1.4.4.

- LZ4 codec updated to 1.9.2.


Changes from 1.16.3 to 1.17.0
=============================

- LZ4 codec updated to 1.9.1.

- Zstd codec updated to 1.4.1.

- BloscLZ codec updated to 2.0.0.  Although this should be fully backward
  compatible, it contains important changes that affects mainly speed, but
  also compression ratios.  Feedback on how it behaves on your own data is
  appreciated.


Changes from 1.16.2 to 1.16.3
=============================

- Fix for building for clang with -march=haswell. See PR #262.

- Fix all the known warnings for GCC/Clang.  Still some work to do for MSVC
  in this front.

- Due to some problems with several CI systems, the check for library symbols
  are deactivated now by default.  If you want to enforce this check, use:
  `cmake .. -DDEACTIVATE_SYMBOLS_CHECK=ON` to re-activate it.


Changes from 1.16.1 to 1.16.2
=============================

- Correct the check for the compressed size when the buffer is memcpyed.  This
  was a regression introduced in 1.16.0.  Fixes #261.


Changes from 1.16.0 to 1.16.1
=============================

- Fixed a regression in 1.16.0 that prevented to compress empty buffers
  (see #260).

- Zstd updated to 1.3.8 (from 1.3.7).


Changes from 1.15.1 to 1.16.0
=============================

- Now the functions that execute Blosc decompressions are safe by default
  for untrusted/possibly corrupted inputs.  The additional checks seem to
  not affect performance significantly (see some benchmarks in #258), so
  this is why they are the default now.

  The previous functions (with less safety) checks are still available with a
  '_unsafe' suffix.  The complete list is:

    - blosc_decompress_unsafe()
    - blosc_decompress_ctx_unsafe()
    - blosc_getitem_unsafe()

  Also, a new API function named blosc_cbuffer_validate(), for validating Blosc
  compressed data, has been added.

  For details, see PR #258.  Thanks to Jeremy Maitin-Shepard.

- Fixed a bug in `blosc_compress()` that could lead to thread deadlock under
  some situations.  See #251.  Thanks to @wenjuno for the report and the fix.

- Fix data race in shuffle.c host_implementation initialization.  Fixes #253.
  Thanks to Jeremy Maitin-Shepard.


Changes from 1.15.0 to 1.15.1
=============================

- Add workaround for Visual Studio 2008's lack of a `stdint.h` file to
  `blosclz.c`.


Changes from 1.14.4 to 1.15.0
=============================

- The `blosc_compress()` and `blosc_decompress()` interfaces are now
  fork-safe, preventing child-process deadlocks in fork-based
  multiprocessing applications. These interfaces with BLOSC_NOLOCK were, and
  continue to be, fork-safe. `_ctx` interface context reuse continues to be
  unsafe in the child process post-fork. See #241.  Thanks to Alex Ford.

- Replaced //-comments with /**/-comments and other improvements for
  compatibility with quite old gcc compilers.  See PR #243.  Thanks to
  Andreas Martin.

- Empty buffers can be compressed again (this was inadvertently prevented while
  fixing #234).  See #247.  Thanks to Valentin Haenel.

- LZ4 internal codec upgraded to 1.8.3 (from 1.8.1.2).

- Zstd internal codec upgraded to 1.3.7 (from 1.3.4).


Changes from 1.14.3 to 1.14.4
=============================

- Added a new `DEACTIVATE_SSE2` option for cmake that is useful for disabling
  SSE2 when doing cross-compilation (see #236).

- New check for detecting output buffers smaller than BLOSC_MAX_OVERHEAD.
  Fixes #234.

- The `complib` and `version` parameters for `blosc_get_complib_info()` can be
  safely set to NULL now.  This allows to call this function even if the user is
  not interested in these parameters (so no need to reserve memory for them).
  Fixes #228.

- In some situations that a supposedly blosc chunk is passed to
  `blosc_decompress()`, one might end with an `Arithmetic exception`.  This
  is probably due to the chunk not being an actual blosc chunk, and divisions
  by zero might occur.  A protection has been added for this. See #237.


Changes from 1.14.2 to 1.14.3
=============================

- Use win32/pthread.c on all Windows builds, even those with GNU compilers.
  Rational: although MinGW provides a more full-featured pthreads replacement,
  it doesn't seem to accomplish anything here since the functionality in
  win32/pthread.c is sufficient for Blosc. Furthermore, using the MinGW
  pthreads adds an additional library dependency to libblosc that is
  annoying for binary distribution. For example, it got in the way of
  distributing cross-compiled Windows binaries for use with Julia, since they
  want the resulting libblosc.dll to be usable on any Windows machine even
  where MinGW is not installed.  See PR #224.  Thanks to Steven G. Johnson.

- Zstd internal sources have been updated to 1.3.4.


Changes from 1.14.1 to 1.14.2
=============================

- Reverted the $Configuration var in CMake configuration for Windows so
  as to restore the compatibility with MS VisualStudio compilers.


Changes from 1.14.0 to 1.14.1
=============================

- Fixed a bug that caused C-Blosc to crash on platforms requiring strict
  alignment (as in some kinds of ARM CPUs).  Fixes #223.  Thanks to Elvis
  Stansvik and Michael Hudson-Doyle for their help.

- Fixed a piece of code that was not C89 compliant.  C89 compliance is
  needed mainly by MS VS2008 which is still used for creating Python 2
  extensions.

- Remove the (spurious) $Configuration var in cmake config for Windows.
  Thanks to Francis Brissette for pointing this out.


Changes from 1.13.7 to 1.14.0
=============================

- New split mode that favors forward compatibility.  That means that,
  from now on, all the buffers created starting with blosc 1.14.0 will
  be forward compatible with any previous versions of the library --at
  least until 1.3.0, when support for multi-codec was introduced.

  So as to select the split mode, a new API function has been introduced:
  https://github.com/Blosc/c-blosc/blob/master/blosc/blosc.h#L500
  Also, the BLOSC_SPLITMODE environment variable is honored when using
  the `blosc_compress()` function.  See
  https://github.com/Blosc/c-blosc/blob/master/blosc/blosc.h#L209

  There is a dedicated blog entry about this at:
  https://www.blosc.org/posts/new-forward-compat-policy/
  More info in PR #216.

  Caveat Emptor: Note that Blosc versions from 1.11.0 to 1.14.0 *might*
  generate buffers that cannot be read with versions < 1.11.0, so if
  forward compatibility is important to you, an upgrade to 1.14.0 is
  recommended.

- All warnings during cmake build stage are enabled by default now.
  PR #218.  Thanks to kalvdans.

- Better checks on versions of formats inside Blosc.  PR #219.  Thanks
  to kalvdans.

- The BLOSC_PRINT_SHUFFLE_ACCEL environment variable is honored now.
  This is useful for determining *at runtime* whether the different SIMD
  capabilities (only for x86 kind processors) are available to Blosc to get
  better performance during shuffle/bitshuffle operation.  As an example,
  here it is the normal output for the simple.c example::

    $ ./simple
    Blosc version info: 1.14.0.dev ($Date:: 2018-02-15 #$)
    Compression: 4000000 -> 41384 (96.7x)
    Decompression successful!
    Successful roundtrip!

  and here with the BLOSC_PRINT_SHUFFLE_ACCEL environment variable set::

    $ BLOSC_PRINT_SHUFFLE_ACCEL= ./simple
    Blosc version info: 1.14.0.dev ($Date:: 2018-02-15 #$)
    Shuffle CPU Information:
    SSE2 available: True
    SSE3 available: True
    SSSE3 available: True
    SSE4.1 available: True
    SSE4.2 available: True
    AVX2 available: True
    AVX512BW available: False
    XSAVE available: True
    XSAVE enabled: True
    XMM state enabled: True
    YMM state enabled: True
    ZMM state enabled: False
    Compression: 4000000 -> 41384 (96.7x)
    Decompression successful!
    Successful roundtrip!

  Blosc only currently leverages the SSE2 and AVX2 instruction sets, but
  it can recognize all of the above.  This is useful mainly for debugging.


Changes from 1.13.6 to 1.13.7
=============================

- More tests for binaries in https://bintray.com/blosc/Conan.


Changes from 1.13.5 to 1.13.6
=============================

- More tests for binaries in https://bintray.com/blosc/Conan.


Changes from 1.13.4 to 1.13.5
=============================

- New conan binaries publicly accessible in https://bintray.com/blosc/Conan.
  Still experimental, but feedback is appreciated.


Changes from 1.13.3 to 1.13.4
=============================

- Fixed a buffer overrun that happens when compressing small buffers and
  len(destination_buffer) < (len(source_buffer) + BLOSC_MAX_OVERHEAD).
  Reported by Ivan Smirnov.


Changes from 1.13.2 to 1.13.3
=============================

- Tests work now when external compressors are located in non-system locations.
  Fixes #210.  Thanks to Leif Walsh.


Changes from 1.13.1 to 1.13.2
=============================

- C-Blosc can be compiled on CentOS 6 now.

- LZ4 internal codec upgraded to 1.8.1.


Changes from 1.13.0 to 1.13.1
=============================

- Fixed a bug uncovered by the python-blosc test suite: when a buffer is
  to be copied, then we should reserve space for the header, not block pointers.


Changes from 1.12.1 to 1.13.0
=============================

- Serious optimization of memory copy functions (see new `blosc/fastcopy.c`).
  This benefits the speed of all the codecs, but specially the BloscLZ one.

- As a result of the above, the BloscLZ codec received a new adjustment of
  knobs so that you should expect better compression ratios with it too.

- LZ4 internal sources have been updated to 1.8.0.

- Zstd internal sources have been updated to 1.3.3.


Changes from 1.12.0 to 1.12.1
=============================

- Backported BloscLZ parameters that were fine-tuned for C-Blosc2.
  You should expect better compression ratios and faster operation,
  specially on modern CPUs.  See:
  https://www.blosc.org/posts/blosclz-tuning/


Changes from 1.11.3 to 1.12.0
=============================

- Snappy, Zlib and Zstd codecs are compiled internally now, even if they are
  installed in the machine.  This has been done in order to avoid
  problems in machines having the shared libraries for the codecs
  accessible but not the includes (typical in Windows boxes).  Also,
  the Zstd codec runs much faster when compiled internally.  The
  previous behaviour can be restored by activating the cmake options
  PREFER_EXTERNAL_SNAPPY, PREFER_EXTERNAL_ZLIB and PREFER_EXTERNAL_ZSTD.

- Zstd internal sources have been updated to 1.3.0.


Changes from 1.11.3 to 1.11.4
=============================

- Internal Zstd codec updated to 1.1.4.


Changes from 1.11.2 to 1.11.3
=============================

- Fixed #181: bitshuffle filter for big endian machines.

- Internal Zstd codec updated to 1.1.3.

- New blocksize for complevel 8 in automatic mode.  This should help specially
  the Zstd codec to achieve better compression ratios.


Changes from 1.11.1 to 1.11.2
=============================

- Enabled use as a CMake subproject, exporting shared & static library targets
  for super-projects to use. See PRs #178, #179 and #180.  Thanks to Kevin
  Murray.

- Internal LZ4 codec updated to 1.7.5.

- Internal Zstd codec updated to 1.1.2.


Changes from 1.11.0 to 1.11.1
=============================

- Fixed a bug introduced in 1.11.0 and discovered by pandas test suite. This
  basically prevented to decompress buffers compressed with previous versions of
  C-Blosc. See: https://github.com/Blosc/python-blosc/issues/115


Changes from 1.10.2 to 1.11.0
=============================

- Internal Zstd codec upgraded to 1.0.0.

- New block size computation inherited from C-Blosc2. Benchmarks are saying that
  this benefits mainly to LZ4, LZ4HC, Zlib and Zstd codecs, both in speed and in
  compression ratios (although YMMV for your case).

- Added the @rpath flag in Mac OSX for shared libraries.  Fixes #175.

- Added a fix for VS2008 discovered in: https://github.com/PyTables/PyTables/pull/569/files#diff-953cf824ebfea7208d2a2e312d9ccda2L126

- License changed from MIT to 3-clause BSD style.


Changes from 1.10.1 to 1.10.2
=============================

- Force the use of --std=gnu99 when using gcc.  Fixes #174.


Changes from 1.10.0 to 1.10.1
=============================

- Removed an inconsistent check for C11 (__STDC_VERSION__ >= 201112L and
  _ISOC11_SOURCE) as this seem to pose problems on compilers doing different
  things in this check (e.g. clang). See
  https://github.com/Blosc/bloscpack/issues/50.


Changes from 1.9.3 to 1.10.0
============================

- Initial support for Zstandard (0.7.4). Zstandard (or Zstd for short) is a new
  compression library that allows better compression than Zlib, but that works
  typically faster (and some times much faster), making of it a good match for
  Blosc.

  Although the Zstd format is considered stable
  (https://fastcompression.blogspot.com/2016_07_03_archive.html), its API is
  maturing very fast, and despite passing the extreme test suite for C-Blosc,
  this codec should be considered in beta for C-Blosc usage purposes. Please
  test it and report back any possible issues you may get.


Changes from 1.9.2 to 1.9.3
===========================

- Reverted a mistake introduced in 1.7.1.  At that time, bit-shuffling
  was enabled for typesize == 1 (i.e. strings), but the change also
  included byte-shuffling accidentally.  This only affected performance,
  but in a quite bad way (a copy was needed).  This has been fixed and
  byte-shuffling is not active when typesize == 1 anymore.


Changes from 1.9.1 to 1.9.2
===========================

- Check whether Blosc is actually initialized before blosc_init(),
  blosc_destroy() and blosc_free_resources().  This makes the library
  more resistant to different initialization cycles
  (e.g. https://github.com/stevengj/Blosc.jl/issues/19).


Changes from 1.9.0 to 1.9.1
===========================

- The internal copies when clevel=0 are made now via memcpy().  At the
  beginning of C-Blosc development, benchmarks where saying that the
  internal, multi-threaded copies inside C-Blosc were faster than
  memcpy(), but 6 years later, memcpy() made greats strides in terms
  of efficiency.  With this, you should expect an slight speed
  advantage (10% ~ 20%) when C-Blosc is used as a replacement of
  memcpy() (which should not be the most common scenario out there).

- Added a new DEACTIVATE_AVX2 cmake option to explicitly disable AVX2
  at build-time.  Thanks to James Bird.

- The ``make -jN`` for parallel compilation should work now.  Thanks
  to James Bird.


Changes from 1.8.1 to 1.9.0
===========================

* New blosc_get_nthreads() function to get the number of threads that
  will be used internally during compression/decompression (set by
  already existing blosc_set_nthreads()).

* New blosc_get_compressor() function to get the compressor that will
  be used internally during compression (set by already existing
  blosc_set_compressor()).

* New blosc_get_blocksize() function to get the internal blocksize to
  be used during compression (set by already existing
  blosc_set_blocksize()).

* Now, when the BLOSC_NOLOCK environment variable is set (to any
  value), the calls to blosc_compress() and blosc_decompress() will
  call blosc_compress_ctx() and blosc_decompress_ctx() under the hood
  so as to avoid the internal locks.  See blosc.h for details.  This
  allows multi-threaded apps calling the non _ctx() functions to avoid
  the internal locks in C-Blosc.  For the not multi-threaded app
  though, it is in general slower to call the _ctx() functions so the
  use of BLOSC_NOLOCK is discouraged.

* In the same vein, from now on, when the BLOSC_NTHREADS environment
  variable is set to an integer, every call to blosc_compress() and
  blosc_decompress() will call blosc_set_nthreads(BLOSC_NTHREADS)
  before the actual compression/decompression process.  See blosc.h
  for details.

* Finally, if BLOSC_CLEVEL, BLOSC_SHUFFLE, BLOSC_TYPESIZE and/or
  BLOSC_COMPRESSOR variables are set in the environment, these will be
  also honored before calling blosc_compress().

* Calling blosc_init() before any other Blosc call, although
  recommended, is not necessary anymore.  The idea is that you can use
  just the basic blosc_compress() and blosc_decompress() and control
  other parameters (nthreads, compressor, blocksize) by using
  environment variables (see above).


Changes from 1.8.0 to 1.8.1
===========================

* Disable the use of __builtin_cpu_supports() for GCC 5.3.1
  compatibility.  Details in:
  https://lists.fedoraproject.org/archives/list/devel@lists.fedoraproject.org/thread/ZM2L65WIZEEQHHLFERZYD5FAG7QY2OGB/


Changes from 1.7.1 to 1.8.0
===========================

* The code is (again) compatible with VS2008 and VS2010.  This is
  important for compatibility with Python 2.6/2.7/3.3/3.4.

* Introduced a new global lock during blosc_decompress() operation.
  As the blosc_compress() was already guarded by a global lock, this
  means that the compression/decompression is again thread safe.
  However, when using C-Blosc from multi-threaded environments, it is
  important to keep using the *_ctx() functions for performance
  reasons.  NOTE: _ctx() functions will be replaced by more powerful
  ones in C-Blosc 2.0.


Changes from 1.7.0 to 1.7.1
===========================

* Fixed a bug preventing bitshuffle to work correctly on getitem().
  Now, everything with bitshuffle seems to work correctly.

* Fixed the thread initialization for blosc_decompress_ctx().  Issue
  #158.  Thanks to Chris Webers.

* Fixed a bug in the blocksize computation introduced in 1.7.0.  This
  could have been creating segfaults.

* Allow bitshuffle to run on 1-byte typesizes.

* New parametrization of the blocksize to be independent of the
  typesize.  This allows a smoother speed throughout all typesizes.

* lz4 and lz4hc codecs upgraded to 1.7.2 (from 1.7.0).

* When calling set_nthreads() but not actually changing the number of
  threads in the internal pool does not teardown and setup it anymore.
  PR #153.  Thanks to Santi Villalba.


Changes from 1.6.1 to 1.7.0
===========================

* Added a new 'bitshuffle' filter so that the shuffle takes place at a
  bit level and not just at a byte one, which is what it does the
  previous 'shuffle' filter.

  For activating this new bit-level filter you only have to pass the
  symbol BLOSC_BITSHUFFLE to `blosc_compress()`.  For the previous
  byte-level one, pass BLOSC_SHUFFLE.  For disabling the shuffle, pass
  BLOSC_NOSHUFFLE.

  This is a port of the existing filter in
  https://github.com/kiyo-masui/bitshuffle.  Thanks to Kiyo Masui for
  changing the license and allowing its inclusion here.

* New acceleration mode for LZ4 and BloscLZ codecs that enters in
  operation with complevel < 9.  This allows for an important boost in
  speed with minimal compression ratio loss.  Francesc Alted.

* LZ4 codec updated to 1.7.0 (r130).

* PREFER_EXTERNAL_COMPLIBS cmake option has been removed and replaced
  by the more fine grained PREFER_EXTERNAL_LZ4, PREFER_EXTERNAL_SNAPPY
  and PREFER_EXTERNAL_ZLIB.  In order to allow the use of the new API
  introduced in LZ4 1.7.0, PREFER_EXTERNAL_LZ4 has been set to OFF by
  default, whereas PREFER_EXTERNAL_SNAPPY and PREFER_EXTERNAL_ZLIB
  continues to be ON.

* Implemented SSE2 shuffle support for buffers containing a number of
  elements which is not a multiple of (typesize * vectorsize).  Jack
  Pappas.

* Added SSE2 shuffle/unshuffle routines for types larger than 16
  bytes.  Jack Pappas.

* 'test_basic' suite has been split in components for a much better
  granularity on what's a possibly failing test.  Also, lots of new
  tests have been added.  Jack Pappas.

* Fixed compilation on non-Intel archs (tested on ARM).  Zbyszek
  Szmek.

* Modifyied cmake files in order to inform that AVX2 on Visual Studio
  is supported only in 2013 update 2 and higher.

* Added a replacement for stdbool.h for Visual Studio < 2013.

* blosclz codec adds Win64/Intel as a platform supporting unaligned
  addressing.  That leads to a speed-up of 2.2x in decompression.

* New blosc_get_version_string() function for retrieving the version
  of the c-blosc library.  Useful when linking with dynamic libraries
  and one want to know its version.

* New example (win-dynamic-linking.c) that shows how to link a Blosc
  DLL dynamically in run-time (Windows only).

* The `context.threads_started` is initialized now when decompressing.
  This could cause crashes in case you decompressed before compressing
  (e.g. directly deserializing blosc buffers).  @atchouprakov.

* The HDF5 filter has been removed from c-blosc and moved into its own
  repo at: https://github.com/Blosc/hdf5

* The MS Visual Studio 2008 has been tested with c-blosc for ensuring
  compatibility with extensions for Python 2.6 and up.


Changes from 1.6.0 to 1.6.1
===========================

* Support for *runtime* detection of AVX2 and SSE2 SIMD instructions.
  These changes make it possible to compile one single binary that
  runs on a system that supports SSE2 or AVX2 (or neither), so the
  redistribution problem is fixed (see #101).  Thanks to Julian Taylor
  and Jack Pappas.

* Added support for MinGW and TDM-GCC compilers for Windows.  Thanks
  to yasushima-gd.

* Fixed a bug in blosclz that could potentially overwrite an area
  beyond the output buffer.  See #113.

* New computation for blocksize so that larger typesizes (> 8 bytes)
  would benefit of much better compression ratios.  Speed is not
  penalized too much.

* New parametrization of the hash table for blosclz codec.  This
  allows better compression in many scenarios, while slightly
  increasing the speed.


Changes from 1.5.4 to 1.6.0
===========================

* Support for AVX2 is here!  The benchmarks with a 4-core Intel
  Haswell machine tell that both compression and decompression are
  accelerated around a 10%, reaching peaks of 9.6 GB/s during
  compression and 26 GB/s during decompression (memcpy() speed for
  this machine is 7.5 GB/s for writes and 11.7 GB/s for reads).  Many
  thanks to @littlezhou for this nice work.

* Support for HPET (high precision timers) for the `bench` program.
  This is particularly important for microbenchmarks like bench is
  doing; since they take so little time to run, the granularity of a
  less-accurate timer may account for a significant portion of the
  runtime of the benchmark itself, skewing the results.  Thanks to
  Jack Pappas.


Changes from 1.5.3 to 1.5.4
===========================

* Updated to LZ4 1.6.0 (r128).

* Fix resource leak in t_blosc.  Jack Pappas.

* Better checks during testing.  Jack Pappas.

* Dynamically loadable HDF5 filter plugin. Kiyo Masui.


Changes from 1.5.2 to 1.5.3
===========================

* Use llabs function (where available) instead of abs to avoid
  truncating the result.  Jack Pappas.

* Use C11 aligned_alloc when it's available.  Jack Pappas.

* Use the built-in stdint.h with MSVC when available.  Jack Pappas.

* Only define the __SSE2__ symbol when compiling with MS Visual C++
  and targeting x64 or x86 with the correct /arch flag set. This
  avoids re-defining the symbol which makes other compilers issue
  warnings.  Jack Pappas.

* Reinitializing Blosc during a call to set_nthreads() so as to fix
  problems with contexts.  Francesc Alted.



Changes from 1.5.1 to 1.5.2
===========================

* Using blosc_compress_ctx() / blosc_decompress_ctx() inside the HDF5
  compressor for allowing operation in multiprocess scenarios.  See:
  https://github.com/PyTables/PyTables/issues/412

  The drawback of this quick fix is that the Blosc filter will be only
  able to use a single thread until another solution can be devised.


Changes from 1.5.0 to 1.5.1
===========================

* Updated to LZ4 1.5.0.  Closes #74.

* Added the 'const' qualifier to non SSE2 shuffle functions. Closes #75.

* Explicitly call blosc_init() in HDF5 blosc_filter.c, fixing a
  segfault.

* Quite a few improvements in cmake files for HDF5 support.  Thanks to
  Dana Robinson (The HDF Group).

* Variable 'class' caused problems compiling the HDF5 filter with g++.
  Thanks to Laurent Chapon.

* Small improvements on docstrings of c-blosc main functions.


Changes from 1.4.1 to 1.5.0
===========================

* Added new calls for allowing Blosc to be used *simultaneously*
  (i.e. lock free) from multi-threaded environments.  The new
  functions are:

  - blosc_compress_ctx(...)
  - blosc_decompress_ctx(...)

  See the new docstrings in blosc.h for how to use them.  The previous
  API should be completely unaffected.  Thanks to Christopher Speller.

* Optimized copies during BloscLZ decompression.  This can make BloscLZ
  to decompress up to 1.5x faster in some situations.

* LZ4 and LZ4HC compressors updated to version 1.3.1.

* Added an examples directory on how to link apps with Blosc.

* stdlib.h moved from blosc.c to blosc.h as suggested by Rob Lathm.

* Fix a warning for {snappy,lz4}-free compilation.  Thanks to Andrew Schaaf.

* Several improvements for CMakeLists.txt (cmake).

* Fixing C99 compatibility warnings.  Thanks to Christopher Speller.


Changes from 1.4.0 to 1.4.1
===========================

* Fixed a bug in blosc_getitem() introduced in 1.4.0.  Added a test for
  blosc_getitem() as well.


Changes from 1.3.6 to 1.4.0
===========================

* Support for non-Intel and non-SSE2 architectures has been added.  In
  particular, the Raspberry Pi platform (ARM) has been tested and all
  tests pass here.

* Architectures requiring strict access alignment are supported as well.
  Due to this, architectures with a high penalty in accessing unaligned
  data (e.g. Raspberry Pi, ARMv6) can compress up to 2.5x faster.

* LZ4 has been updated to r119 (1.2.0) so as to fix a possible security
  breach.


Changes from 1.3.5 to 1.3.6
===========================

* Updated to LZ4 r118 due to a (highly unlikely) security hole.  For
  details see:

  http://blog.securitymouse.com/2014/06/raising-lazarus-20-year-old-bug-that.html


Changes from 1.3.4 to 1.3.5
===========================

* Removed a pointer from 'pointer from integer without a cast' compiler
  warning due to a bad macro definition.


Changes from 1.3.3 to 1.3.4
===========================

* Fixed a false buffer overrun condition.  This bug made c-blosc to
  fail, even if the failure was not real.

* Fixed the type of a buffer string.


Changes from 1.3.2 to 1.3.3
===========================

* Updated to LZ4 1.1.3 (improved speed for 32-bit platforms).

* Added a new `blosc_cbuffer_complib()` for getting the compression
  library for a compressed buffer.


Changes from 1.3.1 to 1.3.2
===========================

* Fix for compiling Snappy sources against MSVC 2008.  Thanks to Mark
  Wiebe!

* Version for internal LZ4 and Snappy are now supported.  When compiled
  against the external libraries, this info is not available because
  they do not support the symbols (yet).


Changes from 1.3.0 to 1.3.1
===========================

* Fixes for a series of issues with the filter for HDF5 and, in
  particular, a problem in the decompression buffer size that made it
  impossible to use the blosc_filter in combination with other ones
  (e.g. fletcher32).  See
  https://github.com/PyTables/PyTables/issues/21.

  Thanks to Antonio Valentino for the fix!


Changes from 1.2.4 to 1.3.0
===========================

A nice handful of compressors have been added to Blosc:

* LZ4 (https://lz4.org/: A very fast
  compressor/decompressor.  Could be thought as a replacement of the
  original BloscLZ, but it can behave better is some scenarios.

* LZ4HC (https://lz4.org/): This is a variation of LZ4
  that achieves much better compression ratio at the cost of being
  much slower for compressing.  Decompression speed is unaffected (and
  sometimes better than when using LZ4 itself!), so this is very good
  for read-only datasets.

* Snappy (https://google.github.io/snappy/): A very fast
  compressor/decompressor.  Could be thought as a replacement of the
  original BloscLZ, but it can behave better is some scenarios.

* Zlib (https://zlib.net/): This is a classic.  It achieves very
  good compression ratios, at the cost of speed.  However,
  decompression speed is still pretty good, so it is a good candidate
  for read-only datasets.

With this, you can select the compression library with the new
function::

  int blosc_set_complib(char* complib);

where you pass the library that you want to use (currently "blosclz",
"lz4", "lz4hc", "snappy" and "zlib", but the list can grow in the
future).

You can get more info about compressors support in you Blosc build by
using these functions::

  char* blosc_list_compressors(void);
  int blosc_get_complib_info(char *compressor, char **complib, char **version);


Changes from 1.2.2 to 1.2.3
===========================

- Added a `blosc_init()` and `blosc_destroy()` so that the global lock
  can be initialized safely.  These new functions will also allow other
  kind of initializations/destructions in the future.

  Existing applications using Blosc do not need to start using the new
  functions right away, as long as they calling `blosc_set_nthreads()`
  previous to anything else.  However, using them is highly recommended.

  Thanks to Oscar Villellas for the init/destroy suggestion, it is a
  nice idea!


Changes from 1.2.1 to 1.2.2
===========================

- All important warnings removed for all tested platforms.  This will
  allow less intrusiveness compilation experiences with applications
  including Blosc source code.

- The `bench/bench.c` has been updated so that it can be compiled on
  Windows again.

- The new web site has been set to: https://www.blosc.org


Changes from 1.2 to 1.2.1
=========================

- Fixed a problem with global lock not being initialized.  This
  affected mostly to Windows platforms.  Thanks to Christoph
  Gohlke for finding the cure!


Changes from 1.1.5 to 1.2
=========================

- Now it is possible to call Blosc simultaneously from a parent threaded
  application without problems.  This has been solved by setting a
  global lock so that the different calling threads do not execute Blosc
  routines at the same time.  Of course, real threading work is still
  available *inside* Blosc itself.  Thanks to Thibault North.

- Support for cmake is now included.  Linux, Mac OSX and Windows
  platforms are supported.  Thanks to Thibault North, Antonio Valentino
  and Mark Wiebe.

- Fixed many compilers warnings (specially about unused variables).

- As a consequence of the above, as minimal change in the API has been
  introduced.  That is, the previous API::

    void blosc_free_resources(void)

  has changed to::

    int blosc_free_resources(void)

  Now, a return value of 0 means that the resources have been released
  successfully.  If the return value is negative, then it is not
  guaranteed that all the resources have been freed.

- Many typos were fixed and docs have been improved.  The script for
  generating nice plots for the included benchmarks has been improved
  too.  Thanks to Valetin Haenel.


Changes from 1.1.4 to 1.1.5
===========================

- Fix compile error with msvc compilers (Christoph Gohlke)


Changes from 1.1.3 to 1.1.4
===========================

- Redefinition of the BLOSC_MAX_BUFFERSIZE constant as (INT_MAX -
  BLOSC_MAX_OVERHEAD) instead of just INT_MAX.  This prevents to produce
  outputs larger than INT_MAX, which is not supported.

- `exit()` call has been replaced by a ``return -1`` in blosc_compress()
  when checking for buffer sizes.  Now programs will not just exit when
  the buffer is too large, but return a negative code.

- Improvements in explicit casts.  Blosc compiles without warnings
  (with GCC) now.

- Lots of improvements in docs, in particular a nice ascii-art diagram
  of the Blosc format (Valentin Haenel).

- Improvements to the plot-speeds.py (Valentin Haenel).

- [HDF5 filter] Adapted HDF5 filter to use HDF5 1.8 by default
  (Antonio Valentino).

- [HDF5 filter] New version of H5Z_class_t definition (Antonio Valentino).


Changes from 1.1.2 to 1.1.3
===========================

- Much improved compression ratio when using large blocks (> 64 KB) and
  high compression levels (> 6) under some circumstances (special data
  distribution).  Closes #7.


Changes from 1.1.1 to 1.1.2
===========================

- Fixes for small typesizes (#6 and #1 of python-blosc).


Changes from 1.1 to 1.1.1
=========================

- Added code to avoid calling blosc_set_nthreads more than necessary.
  That will improve performance up to 3x or more, specially for small
  chunksizes (< 1 MB).


Changes from 1.0 to 1.1
=======================

- Added code for emulating pthreads API on Windows.  No need to link
  explicitly with pthreads lib on Windows anymore.  However, performance
  is a somewhat worse because the new emulation layer does not support
  the `pthread_barrier_wait()` call natively.  But the big improvement
  in installation easiness is worth this penalty (most specially on
  64-bit Windows, where pthreads-win32 support is flaky).

- New BLOSC_MAX_BUFFERSIZE, BLOSC_MAX_TYPESIZE and BLOSC_MAX_THREADS
  symbols are available in blosc.h.  These can be useful for validating
  parameters in clients.  Thanks to Robert Smallshire for suggesting
  that.

- A new BLOSC_MIN_HEADER_LENGTH symbol in blosc.h tells how many bytes
  long is the minimum length of a Blosc header.  `blosc_cbuffer_sizes()`
  only needs these bytes to be passed to work correctly.

- Removed many warnings (related with potentially dangerous type-casting
  code) issued by MSVC 2008 in 64-bit mode.

- Fixed a problem with the computation of the blocksize in the Blosc
  filter for HDF5.

- Fixed a problem with large datatypes.

- Now Blosc is able to work well even if you fork an existing process
  with a pool of threads.  Bug discovered when PyTables runs in
  multiprocess environments.

- Added a new `blosc_getitem()` call to allow the retrieval of items in
  sizes smaller than the complete buffer.  That is useful for the carray
  project, but certainly for others too.


Changes from 0.9.5 to 1.0
=========================

- Added a filter for HDF5 so that people can use Blosc outside PyTables,
  if they want to.

- Many small improvements, specially in README files.

- Do not assume that size_t is uint_32 for every platform.

- Added more protection for large buffers or in allocation memory
  routines.

- The src/ directory has been renamed to blosc/.

- The `maxbytes` parameter in `blosc_compress()` has been renamed to
  `destsize`.  This is for consistency with the `blosc_decompress()`
  parameters.


Changes from 0.9.4 to 0.9.5
===========================

- Now, compression level 0 is allowed, meaning not compression at all.
  The overhead of this mode will be always BLOSC_MAX_OVERHEAD (16)
  bytes.  This mode actually represents using Blosc as a basic memory
  container.

- Supported a new parameter `maxbytes` for ``blosc_compress()``.  It
  represents a maximum of bytes for output.  Tests unit added too.

- Added 3 new functions for querying different metadata on compressed
  buffers.  A test suite for testing the new API has been added too.


Changes from 0.9.3 to 0.9.4
===========================

- Support for cross-platform big/little endian compatibility in Blosc
  headers has been added.

- Fixed several failures exposed by the extremesuite.  The problem was a
  bad check for limits in the buffer size while compressing.

- Added a new suite in bench.c called ``debugsuite`` that is
  appropriate for debugging purposes.  Now, the ``extremesuite`` can be
  used for running the complete (and extremely long) suite.


Changes from 0.9.0 to 0.9.3
===========================

- Fixed several nasty bugs uncovered by the new suites in bench.c.
  Thanks to Tony Theodore and Gabriel Beckers for their (very)
  responsive beta testing and feedback.

- Added several modes (suites), namely ``suite``, ``hardsuite`` and
  ``extremehardsuite`` in bench.c so as to allow different levels of
  testing.


Changes from 0.8.0 to 0.9
=========================

- Internal format version bumped to 2 in order to allow an easy way to
  indicate that a buffer is being saved uncompressed.  This is not
  supported yet, but it might be in the future.

- Blosc can use threads now for leveraging the increasing number of
  multi-core processors out there.  See README-threaded.txt for more
  info.

- Added a protection for MacOSX so that it has to not link against
  posix_memalign() function, which seems not available in old versions of
  MacOSX (for example, Tiger).  At nay rate, posix_memalign() is not
  necessary on Mac because 16 bytes alignment is ensured by default.
  Thanks to Ivan Vilata.  Fixes #3.
