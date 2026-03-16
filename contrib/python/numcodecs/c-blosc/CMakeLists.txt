# CMake build system for Blosc
# ============================
#
# Available options:
#
#   BUILD_STATIC: default ON
#       build the static version of the Blosc library
#   BUILD_SHARED: default ON
#       build the shared library version of the Blosc library
#   BUILD_TESTS: default ON
#       build test programs and generates the "test" target
#   BUILD_FUZZERS: default ON
#       build fuzz test programs and generates the "fuzz" target
#   BUILD_BENCHMARKS: default ON
#       build the benchmark program
#   HIDE_SYMBOLS: default ON
#       hide the symbols that aren't specifically exported
#   DEACTIVATE_SSE2: default OFF
#       do not attempt to build with SSE2 instructions
#   DEACTIVATE_AVX2: default OFF
#       do not attempt to build with AVX2 instructions
#   DEACTIVATE_LZ4: default OFF
#       do not include support for the LZ4 library
#   DEACTIVATE_SNAPPY: default ON
#       do not include support for the Snappy library
#   DEACTIVATE_ZLIB: default OFF
#       do not include support for the Zlib library
#   DEACTIVATE_ZSTD: default OFF
#       do not include support for the Zstd library
#   DEACTIVATE_SYMBOLS_CHECK: default ON
#       do not check for symbols in shared or static libraries
#   PREFER_EXTERNAL_LZ4: default OFF
#       when found, use the installed LZ4 libs instead of included
#       sources
#   PREFER_EXTERNAL_ZLIB: default OFF
#       when found, use the installed Zlib libs instead of included
#       sources
#   PREFER_EXTERNAL_ZSTD: default OFF
#       when found, use the installed Zstd libs instead of included
#       sources
#   TEST_INCLUDE_BENCH_SHUFFLE_1: default ON
#       add a test that runs the benchmark program passing "shuffle" with 1
#       thread as second parameter
#   TEST_INCLUDE_BENCH_SHUFFLE_N: default ON
#       add a test that runs the benchmark program passing "shuffle" with all
#       threads as second parameter
#   TEST_INCLUDE_BENCH_BITSHUFFLE_1: default ON
#       add a test that runs the benchmark program passing "bitshuffle" with 1
#       thread as second parameter
#   TEST_INCLUDE_BENCH_BITSHUFFLE_N: default ON
#       add a test that runs the benchmark program passing "bitshuffle" with
#       all threads as second parameter
#   TEST_INCLUDE_BENCH_SUITE: default OFF
#       add a test that runs the benchmark program passing "suite"
#       as first parameter
#   TEST_INCLUDE_BENCH_SUITE_PARALLEL: default OFF
#       add a test that runs the benchmark program passing "parallel"
#       as first parameter
#   TEST_INCLUDE_BENCH_HARDSUITE: default OFF
#       add a test that runs the benchmark program passing "hardsuite"
#       as first parameter
#   TEST_INCLUDE_BENCH_EXTREMESUITE: default OFF
#       add a test that runs the benchmark program passing "extremesuite"
#       as first parameter
#   TEST_INCLUDE_BENCH_DEBUGSUITE: default OFF
#       add a test that runs the benchmark program passing "debugsuite"
#       as first parameter
#
# Components:
#
#    LIB: includes blosc.so
#    DEV: static includes blosc.a and blosc.h


# Recent versions of cmake dropped compatibility with < 3.5
cmake_minimum_required(VERSION 3.5)

# parse the full version numbers from blosc.h
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/blosc/blosc.h _blosc_h_contents)
string(REGEX REPLACE ".*#define[ \t]+BLOSC_VERSION_MAJOR[ \t]+([0-9]+).*"
     "\\1" BLOSC_VERSION_MAJOR ${_blosc_h_contents})
string(REGEX REPLACE ".*#define[ \t]+BLOSC_VERSION_MINOR[ \t]+([0-9]+).*"
    "\\1" BLOSC_VERSION_MINOR ${_blosc_h_contents})
string(REGEX REPLACE ".*#define[ \t]+BLOSC_VERSION_RELEASE[ \t]+([0-9]+).*"
    "\\1" BLOSC_VERSION_PATCH ${_blosc_h_contents})
string(REGEX REPLACE ".*#define[ \t]+BLOSC_VERSION_STRING[ \t]+\"([-0-9A-Za-z.]+)\".*"
    "\\1" BLOSC_VERSION_STRING ${_blosc_h_contents})

message("Configuring for Blosc version: " ${BLOSC_VERSION_STRING})

# The project can have ASM (zstd 1.5.2 is starting to use it)
if (POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
    project(blosc LANGUAGES C ASM VERSION "${BLOSC_VERSION_MAJOR}.${BLOSC_VERSION_MINOR}.${BLOSC_VERSION_PATCH}")
else()
    project(blosc LANGUAGES C ASM)
endif()

# options
option(BUILD_STATIC
    "Build a static version of the blosc library." ON)
option(BUILD_SHARED
    "Build a shared library version of the blosc library." ON)
option(BUILD_TESTS
    "Build test programs from the blosc compression library" ON)
option(BUILD_FUZZERS
    "Build fuzzer programs from the blosc compression library" ${BUILD_STATIC})
# Hide symbols by default unless they're specifically exported.
# This makes it easier to keep the set of exported symbols the
# same across all compilers/platforms.
option(HIDE_SYMBOLS
    "Build a libraries with hidden symbols unless they're specifically exported" ON)
option(BUILD_BENCHMARKS
    "Build benchmark programs from the blosc compression library" ON)
option(DEACTIVATE_SSE2
    "Do not attempt to build with SSE2 instructions" OFF)
option(DEACTIVATE_AVX2
    "Do not attempt to build with AVX2 instructions" OFF)
option(DEACTIVATE_LZ4
    "Do not include support for the LZ4 library." OFF)
option(DEACTIVATE_SNAPPY
    "Do not include support for the Snappy library." ON)
option(DEACTIVATE_ZLIB
    "Do not include support for the Zlib library." OFF)
option(DEACTIVATE_ZSTD
    "Do not include support for the Zstd library." OFF)
option(DEACTIVATE_SYMBOLS_CHECK
    "Do not check for symbols in shared or static libraries." ON)
option(PREFER_EXTERNAL_LZ4
    "Find and use external LZ4 library instead of included sources." OFF)
option(PREFER_EXTERNAL_ZLIB
    "Find and use external Zlib library instead of included sources." OFF)
option(PREFER_EXTERNAL_ZSTD
    "Find and use external Zstd library instead of included sources." OFF)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
include(GNUInstallDirs)

if(NOT DEACTIVATE_LZ4)
    if(PREFER_EXTERNAL_LZ4)
        find_package(LZ4)
    else()
        message(STATUS "Using LZ4 internal sources.")
    endif()
    # HAVE_LZ4 will be set to true because even if the library is
    # not found, we will use the included sources for it
    set(HAVE_LZ4 TRUE)
endif()

if(NOT DEACTIVATE_SNAPPY)
    find_package(Snappy)
    if(SNAPPY_FOUND)
        message(STATUS "Activating support for SNAPPY.")
        set(HAVE_SNAPPY TRUE)
    else()
        message(STATUS "SNAPPY *not* found.  De-activating support for it.")
    endif()
endif()

if(NOT DEACTIVATE_ZLIB)
    # import the ZLIB_ROOT environment variable to help finding the zlib library
    if(PREFER_EXTERNAL_ZLIB)
        set(ZLIB_ROOT $ENV{ZLIB_ROOT})
        find_package(ZLIB)
        if(NOT ZLIB_FOUND )
            message(STATUS "No zlib found.  Using internal sources.")
        endif()
    else()
        message(STATUS "Using zlib internal sources.")
    endif()
    # HAVE_ZLIB will be set to true because even if the library is not found,
    # we will use the included sources for it
    set(HAVE_ZLIB TRUE)
endif()

if(NOT DEACTIVATE_ZSTD)
    if(PREFER_EXTERNAL_ZSTD)
        find_package(Zstd)
    else()
        message(STATUS "Using ZSTD internal sources.")
    endif()
    # HAVE_ZSTD will be set to true because even if the library is
    # not found, we will use the included sources for it
    set(HAVE_ZSTD TRUE)
endif()

# create the config.h file
configure_file("blosc/config.h.in"  "blosc/config.h" )

# now make sure that you set the build directory on your "Include" path when compiling
include_directories("${PROJECT_BINARY_DIR}/blosc/")

# If the build type is not set, default to Release.
set(BLOSC_DEFAULT_BUILD_TYPE Release)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "No build type specified. Defaulting to '${BLOSC_DEFAULT_BUILD_TYPE}'.")
    set(CMAKE_BUILD_TYPE ${BLOSC_DEFAULT_BUILD_TYPE} CACHE STRING
        "Choose the type of build." FORCE)

    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

if(APPLE)
  # enable @rpath in the install name for any shared library being built.  See #175.
  set(CMAKE_MACOSX_RPATH TRUE)
endif()

# Based on the target system's processor and the compiler being used,
# set build variables indicating which hardware features can be targeted
# by the compiler. Note we DO NOT check which hardware features are supported
# by this (the host) system, because we want to be able to support compiling
# for newer hardware on older machines as well as cross-compilation.
message(STATUS "Building for system processor ${CMAKE_SYSTEM_PROCESSOR}")
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(i386|i686|x86_64|amd64|AMD64)")
    if(CMAKE_C_COMPILER_ID STREQUAL GNU)
        # We need C99 (GNU99 more exactly)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu99")
        set(COMPILER_SUPPORT_SSE2 TRUE)
        if(CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.7 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 4.7)
            set(COMPILER_SUPPORT_AVX2 TRUE)
        else()
            set(COMPILER_SUPPORT_AVX2 FALSE)
        endif()
    elseif(CMAKE_C_COMPILER_ID STREQUAL Clang)
        set(COMPILER_SUPPORT_SSE2 TRUE)
        if(CMAKE_C_COMPILER_VERSION VERSION_GREATER 3.2 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 3.2)
            set(COMPILER_SUPPORT_AVX2 TRUE)
        else()
            set(COMPILER_SUPPORT_AVX2 FALSE)
        endif()
    elseif(CMAKE_C_COMPILER_ID STREQUAL Intel)
        set(COMPILER_SUPPORT_SSE2 TRUE)
        if(CMAKE_C_COMPILER_VERSION VERSION_GREATER 14.0 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 14.0)
            set(COMPILER_SUPPORT_AVX2 TRUE)
        else()
            set(COMPILER_SUPPORT_AVX2 FALSE)
        endif()
    elseif(MSVC)
        set(COMPILER_SUPPORT_SSE2 TRUE)
        if(CMAKE_C_COMPILER_VERSION VERSION_GREATER 18.00.30501 OR CMAKE_C_COMPILER_VERSION VERSION_EQUAL 18.00.30501)
            set(COMPILER_SUPPORT_AVX2 TRUE)
        else()
            set(COMPILER_SUPPORT_AVX2 FALSE)
        endif()
    else()
        set(COMPILER_SUPPORT_SSE2 FALSE)
        set(COMPILER_SUPPORT_AVX2 FALSE)
        # Unrecognized compiler. Emit a warning message to let the user know hardware-acceleration won't be available.
        message(WARNING "Unable to determine which ${CMAKE_SYSTEM_PROCESSOR} hardware features are supported by the C compiler (${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}).")
    endif()
else()
    # If the target system processor isn't recognized, emit a warning message to alert the user
    # that hardware-acceleration support won't be available but allow configuration to proceed.
    message(WARNING "Unrecognized system processor ${CMAKE_SYSTEM_PROCESSOR}. Cannot determine which hardware features (${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}) supports, so hardware-accelerated implementations will not be available.")
endif()

# disable SSE2 if specified (useful for cross-compiling, see #236)
if(DEACTIVATE_SSE2)
    set(COMPILER_SUPPORT_SSE2 FALSE)
endif()

# disable AVX2 if specified or if SSE is deactivated
if(DEACTIVATE_AVX2 OR DEACTIVATE_SSE2)
    set(COMPILER_SUPPORT_AVX2 FALSE)
endif()

# flags
# Set -Wall and other useful warning flags.
if(CMAKE_C_COMPILER_ID STREQUAL GNU OR CMAKE_C_COMPILER_ID STREQUAL Clang OR CMAKE_C_COMPILER_ID STREQUAL Intel)
    add_compile_options(-Wall -Wwrite-strings -Wno-unused-function)
endif()

# @NOTE: -O3 is enabled in Release mode (CMAKE_BUILD_TYPE="Release")

# Set the "-msse2" build flag if supported.
if(CMAKE_C_COMPILER_ID STREQUAL GNU OR CMAKE_C_COMPILER_ID STREQUAL Clang OR CMAKE_C_COMPILER_ID STREQUAL Intel)
    if(COMPILER_SUPPORT_SSE2)
        add_compile_options(-msse2)
    endif()
endif()

if(MSVC)
    if(NOT CMAKE_C_FLAGS)
        set(CMAKE_C_FLAGS "/Ox" CACHE STRING "C flags." FORCE)
    endif()

    # Turn off misguided "secure CRT" warnings in MSVC.
    # Microsoft wants people to use the MS-specific <function>_s
    # versions of certain C functions but this is difficult to do
    # in platform-independent code.
    add_definitions( -D_CRT_SECURE_NO_WARNINGS )
endif()

if(WIN32)
    # For some supporting headers
    include_directories("${CMAKE_CURRENT_SOURCE_DIR}/blosc")
endif()

if(HAIKU)
    # Haiku have posix_memalign, required by test_common.h
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_POSIX_C_SOURCE=200112L")
endif()

if(NOT DEFINED BLOSC_IS_SUBPROJECT)
    if("^${CMAKE_SOURCE_DIR}$" STREQUAL "^${PROJECT_SOURCE_DIR}$")
        set(BLOSC_IS_SUBPROJECT FALSE)
    else()
        set(BLOSC_IS_SUBPROJECT TRUE)
        message(STATUS "Detected that BLOSC is used a subproject.")
    endif()
endif()

if(NOT DEFINED BLOSC_INSTALL)
    if(BLOSC_IS_SUBPROJECT)
        set(BLOSC_INSTALL FALSE)
    else()
        set(BLOSC_INSTALL TRUE)
    endif()
endif()


# subdirectories
add_subdirectory(blosc)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
    add_subdirectory(compat)
endif()

if(BUILD_FUZZERS)
    if(NOT BUILD_STATIC)
        message(FATAL_ERROR "BUILD_FUZZERS requires BUILD_STATIC to be enabled.")
    endif()
    enable_testing()
    add_subdirectory(tests/fuzz)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(bench)
endif()


# uninstall target
if(BLOSC_INSTALL)
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/blosc.pc.in"
        "${CMAKE_CURRENT_BINARY_DIR}/blosc.pc"
        @ONLY)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/blosc.pc"
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig" COMPONENT DEV)

    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
        IMMEDIATE @ONLY)
    add_custom_target(uninstall
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif()

# packaging
if(NOT BLOSC_IS_SUBPROJECT)
    include(InstallRequiredSystemLibraries)

    set(CPACK_GENERATOR TGZ ZIP)
    set(CPACK_SOURCE_GENERATOR TGZ ZIP)
    set(CPACK_PACKAGE_VERSION_MAJOR ${BLOSC_VERSION_MAJOR})
    set(CPACK_PACKAGE_VERSION_MINOR ${BLOSC_VERSION_MINOR})
    set(CPACK_PACKAGE_VERSION_PATCH ${BLOSC_VERSION_PATCH})
    set(CPACK_PACKAGE_VERSION ${BLOSC_STRING_VERSION})
    set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
    set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
        "A blocking, shuffling and lossless compression library")
    set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE.txt")
    set(CPACK_SOURCE_IGNORE_FILES "/build.*;.*~;\\\\.git.*;\\\\.DS_Store")
    set(CPACK_STRIP_FILES TRUE)
    set(CPACK_SOURCE_STRIP_FILES TRUE)

    include(CPack)
endif()
