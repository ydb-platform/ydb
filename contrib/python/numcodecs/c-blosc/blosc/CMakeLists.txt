# a simple way to detect that we are using CMAKE
add_definitions(-DUSING_CMAKE)

set(INTERNAL_LIBS ${PROJECT_SOURCE_DIR}/internal-complibs)

if(HIDE_SYMBOLS)
  set(CMAKE_C_VISIBILITY_PRESET hidden)
endif(HIDE_SYMBOLS)

# includes
set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR})
if(NOT DEACTIVATE_LZ4)
    if (LZ4_FOUND)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${LZ4_INCLUDE_DIR})
    else(LZ4_FOUND)
        set(LZ4_LOCAL_DIR ${INTERNAL_LIBS}/lz4-1.10.0)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${LZ4_LOCAL_DIR})
    endif(LZ4_FOUND)
endif(NOT DEACTIVATE_LZ4)

if(NOT DEACTIVATE_SNAPPY)
    if (SNAPPY_FOUND)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${SNAPPY_INCLUDE_DIR})
    else(SNAPPY_FOUND)
        set(SNAPPY_LOCAL_DIR ${INTERNAL_LIBS}/snappy-1.1.1)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${SNAPPY_LOCAL_DIR})
    endif(SNAPPY_FOUND)
endif(NOT DEACTIVATE_SNAPPY)

if(NOT DEACTIVATE_ZLIB)
    if (ZLIB_FOUND)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${ZLIB_INCLUDE_DIR})
    else(ZLIB_FOUND)
        set(ZLIB_LOCAL_DIR ${INTERNAL_LIBS}/zlib-1.3.1)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${ZLIB_LOCAL_DIR})
    endif(ZLIB_FOUND)
endif(NOT DEACTIVATE_ZLIB)

if (NOT DEACTIVATE_ZSTD)
    if (ZSTD_FOUND)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${ZSTD_INCLUDE_DIR})
    else (ZSTD_FOUND)
        set(ZSTD_LOCAL_DIR ${INTERNAL_LIBS}/zstd-1.5.6)
        set(BLOSC_INCLUDE_DIRS ${BLOSC_INCLUDE_DIRS} ${ZSTD_LOCAL_DIR} ${ZSTD_LOCAL_DIR}/common)
    endif (ZSTD_FOUND)
endif (NOT DEACTIVATE_ZSTD)

include_directories(${BLOSC_INCLUDE_DIRS})

# library sources
set(SOURCES blosc.c blosclz.c fastcopy.c shuffle-generic.c bitshuffle-generic.c
        blosc-common.h blosc-export.h)
if(COMPILER_SUPPORT_SSE2)
    message(STATUS "Adding run-time support for SSE2")
    set(SOURCES ${SOURCES} shuffle-sse2.c bitshuffle-sse2.c)
endif(COMPILER_SUPPORT_SSE2)
if(COMPILER_SUPPORT_AVX2)
    message(STATUS "Adding run-time support for AVX2")
    set(SOURCES ${SOURCES} shuffle-avx2.c bitshuffle-avx2.c)
endif(COMPILER_SUPPORT_AVX2)
set(SOURCES ${SOURCES} shuffle.c)

set(version_string ${BLOSC_VERSION_MAJOR}.${BLOSC_VERSION_MINOR}.${BLOSC_VERSION_PATCH})

set(CMAKE_THREAD_PREFER_PTHREAD TRUE) # pre 3.1
set(THREADS_PREFER_PTHREAD_FLAG TRUE) # CMake 3.1+
if(WIN32)
    # try to use the system library
    find_package(Threads)
    if(NOT Threads_FOUND)
        message(STATUS "using the internal pthread library for win32 systems.")
        set(SOURCES ${SOURCES} win32/pthread.c)
    else(NOT Threads_FOUND)
        if(CMAKE_VERSION VERSION_LESS 3.1)
            set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT})
        else()
            set(LIBS ${LIBS} Threads::Threads)
        endif()
    endif(NOT Threads_FOUND)
else(WIN32)
    find_package(Threads REQUIRED)
    if(CMAKE_VERSION VERSION_LESS 3.1)
        set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT})
    else()
        set(LIBS ${LIBS} Threads::Threads)
    endif()
endif(WIN32)

if(NOT DEACTIVATE_LZ4)
    if(LZ4_FOUND)
        set(LIBS ${LIBS} ${LZ4_LIBRARY})
    else(LZ4_FOUND)
        file(GLOB LZ4_FILES ${LZ4_LOCAL_DIR}/*.c)
        set(SOURCES ${SOURCES} ${LZ4_FILES})
        source_group("LZ4" FILES ${LZ4_FILES})
    endif(LZ4_FOUND)
endif(NOT DEACTIVATE_LZ4)

if(NOT DEACTIVATE_SNAPPY)
    if(SNAPPY_FOUND)
        set(LIBS ${LIBS} ${SNAPPY_LIBRARY})
    else(SNAPPY_FOUND)
        file(GLOB SNAPPY_FILES ${SNAPPY_LOCAL_DIR}/*.cc)
        set(SOURCES ${SOURCES} ${SNAPPY_FILES})
        source_group("Snappy" FILES ${SNAPPY_FILES})
    endif(SNAPPY_FOUND)
endif(NOT DEACTIVATE_SNAPPY)

if(NOT DEACTIVATE_ZLIB)
    if(ZLIB_FOUND)
        set(LIBS ${LIBS} ${ZLIB_LIBRARY})
    else(ZLIB_FOUND)
        file(GLOB ZLIB_FILES ${ZLIB_LOCAL_DIR}/*.c)
        set(SOURCES ${SOURCES} ${ZLIB_FILES})
        source_group("Zlib" FILES ${ZLIB_FILES})
    endif(ZLIB_FOUND)
endif(NOT DEACTIVATE_ZLIB)

if (NOT DEACTIVATE_ZSTD)
    if (ZSTD_FOUND)
        set(LIBS ${LIBS} ${ZSTD_LIBRARY})
    else (ZSTD_FOUND)
        # Enable assembly code only when not using MSVC *and* x86 is there
        if((NOT MSVC) AND COMPILER_SUPPORT_SSE2)   # if SSE2 is here, this is an x86 platform
            message(STATUS "Adding support for assembly sources in ZSTD")
            file(GLOB ZSTD_DECOMPRESS_FILES ${ZSTD_LOCAL_DIR}/decompress/*.S)
        else()
            message(STATUS "Disabling support for assembly sources in ZSTD")
            add_compile_definitions("ZSTD_DISABLE_ASM")
        endif()
        file(GLOB ZSTD_DECOMPRESS_FILES ${ZSTD_DECOMPRESS_FILES}
                ${ZSTD_LOCAL_DIR}/decompress/*.c)
        file(GLOB ZSTD_COMMON_FILES ${ZSTD_LOCAL_DIR}/common/*.c)
        file(GLOB ZSTD_COMPRESS_FILES ${ZSTD_LOCAL_DIR}/compress/*.c)
        file(GLOB ZSTD_DICT_FILES ${ZSTD_LOCAL_DIR}/dictBuilder/*.c)
        set(ZSTD_FILES ${ZSTD_COMMON_FILES} ${ZSTD_COMPRESS_FILES}
            ${ZSTD_DECOMPRESS_FILES} ${ZSTD_DICT_FILES})
        set(SOURCES ${SOURCES} ${ZSTD_FILES})
        source_group("Zstd" FILES ${ZSTD_FILES})
    endif (ZSTD_FOUND)
endif (NOT DEACTIVATE_ZSTD)


# targets
if (BUILD_SHARED)
    add_library(blosc_shared SHARED ${SOURCES})
    set_target_properties(blosc_shared PROPERTIES OUTPUT_NAME blosc)
    set_target_properties(blosc_shared PROPERTIES
            VERSION ${version_string}
            SOVERSION 1  # Change this when an ABI change happens
        )
    set_property(
        TARGET blosc_shared
        APPEND PROPERTY COMPILE_DEFINITIONS BLOSC_SHARED_LIBRARY)
endif()

# Based on the target architecture and hardware features supported
# by the C compiler, set hardware architecture optimization flags
# for specific shuffle implementations.
if(COMPILER_SUPPORT_SSE2)
    if (MSVC)
        # MSVC targets SSE2 by default on 64-bit configurations, but not 32-bit configurations.
        if (${CMAKE_SIZEOF_VOID_P} EQUAL 4)
            set_source_files_properties(shuffle-sse2.c bitshuffle-sse2.c PROPERTIES COMPILE_FLAGS "/arch:SSE2")
        endif (${CMAKE_SIZEOF_VOID_P} EQUAL 4)
    else (MSVC)
        set_source_files_properties(shuffle-sse2.c bitshuffle-sse2.c PROPERTIES COMPILE_FLAGS -msse2)
    endif (MSVC)

    # Define a symbol for the shuffle-dispatch implementation
    # so it knows SSE2 is supported even though that file is
    # compiled without SSE2 support (for portability).
    set_property(
        SOURCE shuffle.c
        APPEND PROPERTY COMPILE_DEFINITIONS SHUFFLE_SSE2_ENABLED)
endif(COMPILER_SUPPORT_SSE2)
if(COMPILER_SUPPORT_AVX2)
    if (MSVC)
        set_source_files_properties(shuffle-avx2.c bitshuffle-avx2.c
                PROPERTIES COMPILE_FLAGS "/arch:AVX2")
    else (MSVC)
        set_source_files_properties(shuffle-avx2.c bitshuffle-avx2.c
                PROPERTIES COMPILE_FLAGS -mavx2)
    endif (MSVC)

    # Define a symbol for the shuffle-dispatch implementation
    # so it knows AVX2 is supported even though that file is
    # compiled without AVX2 support (for portability).
    set_property(
        SOURCE shuffle.c
        APPEND PROPERTY COMPILE_DEFINITIONS SHUFFLE_AVX2_ENABLED)
endif(COMPILER_SUPPORT_AVX2)

# When the option has been selected to compile the test suite,
# compile an additional version of blosc_shared which exports
# some normally-hidden symbols (to facilitate unit testing).
if (BUILD_TESTS)
    add_library(blosc_shared_testing SHARED ${SOURCES})
    set_target_properties(blosc_shared_testing PROPERTIES OUTPUT_NAME blosc_testing)
    set_property(
        TARGET blosc_shared_testing
        APPEND PROPERTY COMPILE_DEFINITIONS BLOSC_SHARED_LIBRARY)
    set_property(
        TARGET blosc_shared_testing
        APPEND PROPERTY COMPILE_DEFINITIONS BLOSC_TESTING)
endif()

if (BUILD_SHARED)
    target_link_libraries(blosc_shared ${LIBS})
    target_include_directories(blosc_shared PUBLIC ${BLOSC_INCLUDE_DIRS})
endif()

if (BUILD_TESTS)
    target_link_libraries(blosc_shared_testing ${LIBS})
    target_include_directories(blosc_shared_testing PUBLIC ${BLOSC_INCLUDE_DIRS})
endif()

if(BUILD_STATIC)
    add_library(blosc_static STATIC ${SOURCES})
    set_target_properties(blosc_static PROPERTIES OUTPUT_NAME blosc)
    if (MSVC)
        set_target_properties(blosc_static PROPERTIES PREFIX lib)
    endif()
    target_link_libraries(blosc_static ${LIBS})
    target_include_directories(blosc_static PUBLIC ${BLOSC_INCLUDE_DIRS})
endif(BUILD_STATIC)

# install
if(BLOSC_INSTALL)
    install(FILES blosc.h blosc-export.h
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} COMPONENT DEV)
    if(BUILD_SHARED)
        install(TARGETS blosc_shared
                LIBRARY COMPONENT LIB
                ARCHIVE COMPONENT DEV
                RUNTIME COMPONENT LIB)
    endif(BUILD_SHARED)
    if(BUILD_STATIC)
        install(TARGETS blosc_static COMPONENT DEV)
    endif(BUILD_STATIC)
endif(BLOSC_INSTALL)
