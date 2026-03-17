# libaec Changelog
All notable changes to libaec will be documented in this file.

## [1.1.6] - 2026-06-16

### Fixed
- CMake fixes by Adrien Wu
- Buffer overflow in decoder reported by Even Rouault

## [1.1.5] - 2026-01-23

### Changed
- CMake now builds libaec and libsz on MacOS with the same version
  and compatibility version as libtool.

### Fixed
- CMake fixes by Antonio Rojas, Christoph Junghans, Orion Poplawski
  and malcolm-vivosa

## [1.1.4] - 2025-06-12

### Changed
- Build either or both shared and static libraries. The behavior is
  selected by the BUILD_SHARED_LIBS and BUILD_STATIC_LIBS options
  which are both ON by default. For example, add
  -DBUILD_STATIC_LIBS=OFF to cmake to build only the shared libraries
  (Markus Mützel).

### Fixed
- Various improvements for CMake by Markus Mützel, Vincent LE GARREC
  and Miloš Komarčević.

## [1.1.3] - 2024-03-21

### Fixed
- Compiler warnings


## [1.1.2] - 2023-10-04

### Fixed
- Compile issue with MSVC

## [1.1.1] - 2023-09-29

### Fixed
- Offsets when encoding by Eugen Betke

## [1.1.0] - 2023-08-21 (not released)

### Changed
- Rename aec executable to graec. This avoids a name clash with the
  library itself in cmake builds using the Ninja
  generator. Furthermore, the executable and its manual page will not
  be installed any more since it is mainly used for internal testing
  and benchmarking.

- The include file libaec.h now contains version information.

### Added
- Support for decoding data ranges by Eugen Betke. You can now start
  decoding at previously noted offsets of reference samples. New API
  functions allow you to extract possible offsets and decode ranges
  from encoded data.

## [1.0.6] - 2021-09-17

### Changed
- Improved cmake for mingw by Miloš Komarčević

## [1.0.5] - 2021-06-16

### Changed
- Updated documentation to new 121.0-B-3 standard. The new standard
  mainly clarifies and explicitly defines special cases which could be
  ambiguous or misleading in previous revisions.

  These changes did *not* require any substantial changes to libaec.
  Existing compressed data is still compatible with this version of
  the library and compressed data produced by this version can be
  uncompressed with previous versions.

- Modernized CMake

- Better CMake integration with HDF5 by Jan-Willem Blokland

## [1.0.4] - 2019-02-11

### Added
- Test data

### Fixed
- Include file

## [1.0.3] - 2019-02-04

### Changed
- Improvements to testing and fuzzing by Kurt Schwehr

### Fixed
- Various ubsan issues

## [1.0.2] - 2017-10-18

### Fixed
- C99 requirement in all build systems

## [1.0.1] - 2017-07-14

### Fixed
- Potential security vulnerabilities in decoder exposed by libFuzzer.

### Added
- Fuzz target for decoding and encoding.

### Changed
- Improved Cmake support by Christoph Junghans

## [1.0.0] - 2016-11-16

### Added
- Include CCSDS test data with libaec. See THANKS.

### Changed
- Better compatibility with OSX for make check.
- Allow Cygwin to build DLLs.

## [0.3.4] - 2016-08-16

### Fixed
- Pad incomplete last line when in SZ compatibility mode.

## [0.3.3] - 2016-05-12

### Fixed
- Bug with zero blocks in the last RSI (reference sample interval)
when data size is not a multiple of RSIs or segments (64 blocks) and
the zero region reaches a segment boundary.
- More robust error handling.

### Changed
- Vectorization improvement for Intel compiler.
- Better compatibility with netcdf's build process.

## [0.3.2] - 2015-02-04

### Changed
- Allow nonconforming block sizes in SZ mode.
- Performance improvement for decoder.

## [0.3.1] - 2014-10-23

### Fixed
- Allow incomplete scanlines in SZ mode.

## [0.3] - 2014-08-06

### Changed
- Performance improvement for encoding pre-precessed data.
- More efficient coding of second extension if reference sample is
present.
- Port library to Windows (Visual Studio).

### Added
- Support building with CMake.
- Benchmarking target using ECHAM data (make bench).

## [0.2] - 2014-02-12

### Fixed
- Incorrect length calculation in assessment of Second Extension
coding.
- Unlimited encoding of fundamental sequences.
- Handle corrupted compressed data more gracefully.

### Added
- Additional testing with official CCSDS sample data.
- Support restricted coding options from latest standard.

### Changed
- Facilitate generation of SIMD instructions by compiler.

## [0.1] - 2013-05-21

### Added
- Initial release.
