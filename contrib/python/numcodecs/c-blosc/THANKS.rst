I'd like to thank the PyTables community that have collaborated in the
exhaustive testing of Blosc.  With an aggregate amount of more than
300 TB of different datasets compressed *and* decompressed
successfully, I can say that Blosc is pretty safe now and ready for
production purposes.

Other important contributions:

* Valentin Haenel did a terrific work implementing the support for the
  Snappy compression, fixing typos and improving docs and the plotting
  script.

* Thibault North, with ideas from Oscar Villellas, contributed a way
  to call Blosc from different threads in a safe way.  Christopher
  Speller introduced contexts so that a global lock is not necessary
  anymore.

* The CMake support was initially contributed by Thibault North, and
  Antonio Valentino and Mark Wiebe made great enhancements to it.

* Christopher Speller also introduced the two new '_ctx' calls to
  avoid the use of the blosc_init() and blosc_destroy().

* Jack Pappas contributed important portability enhancements,
  specially runtime and cross-platform detection of SSE2/AVX2 as well
  as high precision timers (HPET) for the benchmark program.

* @littlezhou implemented the AVX2 version of shuffle routines.

* Julian Taylor contributed a way to detect AVX2 in runtime and
  calling the appropriate routines only if the underlying hardware
  supports it.

* Kiyo Masui for relicensing his bitshuffle project for allowing the
  inclusion of part of his code in Blosc.
