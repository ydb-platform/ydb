# This CMake file is heavily inspired by following `usearch` CMake:
# https://github.com/nlohmann/json/blob/develop/CMakeLists.txt
cmake_minimum_required(VERSION 3.11 FATAL_ERROR)
project(
    usearch
    VERSION 2.22.0
    LANGUAGES C CXX
    DESCRIPTION "Smaller & Faster Single-File Vector Search Engine from Unum"
    HOMEPAGE_URL "https://github.com/unum-cloud/usearch"
)

# Determine if USearch is built as a subproject (using `add_subdirectory`) or if it is the main project
set(USEARCH_IS_MAIN_PROJECT OFF)

if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(USEARCH_IS_MAIN_PROJECT ON)
endif ()

# Allow CMake 3.13+ to override options when using FetchContent / add_subdirectory
if (POLICY CMP0077)
    cmake_policy(SET CMP0077 NEW)
endif ()

# Options
option(USEARCH_INSTALL "Install CMake targets" OFF)

option(USEARCH_USE_OPENMP "Use OpenMP for a thread pool" OFF)
option(USEARCH_USE_SIMSIMD "Use SimSIMD hardware-accelerated metrics" OFF)
option(USEARCH_USE_JEMALLOC "Use JeMalloc for faster memory allocations" OFF)
option(USEARCH_USE_FP16LIB "Use software emulation for half-precision types" ON)

option(USEARCH_BUILD_TEST_CPP "Compile a native unit test in C++" ${USEARCH_IS_MAIN_PROJECT})
option(USEARCH_BUILD_BENCH_CPP "Compile a native benchmark in C++" ${USEARCH_IS_MAIN_PROJECT})
option(USEARCH_BUILD_LIB_C "Compile a native library for the C 99 interface" OFF)
option(USEARCH_BUILD_TEST_C "Compile a test for the C 99 interface" OFF)
option(USEARCH_BUILD_JNI "Compile JNI library for Java bindings" OFF)
option(USEARCH_BUILD_WOLFRAM "Compile Wolfram Language bindings" OFF)
option(USEARCH_BUILD_SQLITE "Compile separate SQLite extension" OFF)

# Includes
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH})
include(ExternalProject)

# Configuration
include(GNUInstallDirs)
set(USEARCH_TARGET_NAME ${PROJECT_NAME})
set(USEARCH_CONFIG_INSTALL_DIR
    "${CMAKE_INSTALL_DATADIR}/cmake/${PROJECT_NAME}"
    CACHE INTERNAL ""
)
set(USEARCH_INCLUDE_INSTALL_DIR "${CMAKE_INSTALL_INCLUDEDIR}")
set(USEARCH_TARGETS_EXPORT_NAME "${PROJECT_NAME}Targets")
set(USEARCH_CMAKE_CONFIG_TEMPLATE "cmake/config.cmake.in")
set(USEARCH_CMAKE_CONFIG_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(USEARCH_CMAKE_VERSION_CONFIG_FILE "${USEARCH_CMAKE_CONFIG_DIR}/${PROJECT_NAME}ConfigVersion.cmake")
set(USEARCH_CMAKE_PROJECT_CONFIG_FILE "${USEARCH_CMAKE_CONFIG_DIR}/${PROJECT_NAME}Config.cmake")
set(USEARCH_CMAKE_PROJECT_TARGETS_FILE "${USEARCH_CMAKE_CONFIG_DIR}/${PROJECT_NAME}Targets.cmake")
set(USEARCH_PKGCONFIG_INSTALL_DIR "${CMAKE_INSTALL_DATADIR}/pkgconfig")

include(CheckCSourceCompiles)
# Check for `__fp16` support
check_c_source_compiles(
    [=[
int
main(int argc, char *argv)
{
  __fp16 a = 1.0;
  __fp16 b = a + a;
  return 0;
}
]=]
    USEARCH_CAN_COMPILE_FP16
)

# Check for `_Float16` support
check_c_source_compiles(
    [=[
int
main(int argc, char *argv)
{
  _Float16 a = 1.0;
  _Float16 b = a + a;
  return 0;
}
]=]
    USEARCH_CAN_COMPILE_FLOAT16
)

# Check for `__bf16` support
check_c_source_compiles(
    [=[
int
main(int argc, char *argv)
{
  __bf16 a = 1.0;
  __bf16 b = a + a;
  return 0;
}
]=]
    USEARCH_CAN_COMPILE_BF16
)

# Check for `_Bfloat16` support
check_c_source_compiles(
    [=[
int
main(int argc, char *argv)
{
  _Bfloat16 a = 1.0;
  _Bfloat16 b = a + a;
  return 0;
}
]=]
    USEARCH_CAN_COMPILE_BFLOAT16
)

# Define our header-only library
add_library(${USEARCH_TARGET_NAME} INTERFACE)
add_library(${PROJECT_NAME}::${USEARCH_TARGET_NAME} ALIAS ${USEARCH_TARGET_NAME})
set(USEARCH_INCLUDE_BUILD_DIR "${PROJECT_SOURCE_DIR}/include/")

if (${CMAKE_VERSION} VERSION_LESS "3.8.0")
    target_compile_features(${USEARCH_TARGET_NAME} INTERFACE cxx_range_for)
else ()
    target_compile_features(${USEARCH_TARGET_NAME} INTERFACE cxx_std_11)
endif ()

# Core compilation settings affecting "index.hpp"
target_compile_definitions(${USEARCH_TARGET_NAME} INTERFACE "USEARCH_USE_OPENMP=$<BOOL:${USEARCH_USE_OPENMP}>")

# Supplementary compilation settings affecting "index_plugins.hpp"
target_compile_definitions(${USEARCH_TARGET_NAME} INTERFACE "USEARCH_USE_FP16LIB=$<BOOL:${USEARCH_USE_FP16LIB}>")
target_compile_definitions(${USEARCH_TARGET_NAME} INTERFACE "USEARCH_USE_SIMSIMD=$<BOOL:${USEARCH_USE_SIMSIMD}>")

# Define which types can be compiled
target_compile_definitions(
    ${USEARCH_TARGET_NAME} INTERFACE "USEARCH_CAN_COMPILE_FP16=$<BOOL:${USEARCH_CAN_COMPILE_FP16}>"
)
target_compile_definitions(
    ${USEARCH_TARGET_NAME} INTERFACE "USEARCH_CAN_COMPILE_FLOAT16=$<BOOL:${USEARCH_CAN_COMPILE_FLOAT16}>"
)
target_compile_definitions(
    ${USEARCH_TARGET_NAME} INTERFACE "USEARCH_CAN_COMPILE_BF16=$<BOOL:${USEARCH_CAN_COMPILE_BF16}>"
)
target_compile_definitions(
    ${USEARCH_TARGET_NAME} INTERFACE "USEARCH_CAN_COMPILE_BFLOAT16=$<BOOL:${USEARCH_CAN_COMPILE_BFLOAT16}>"
)

target_include_directories(
    ${USEARCH_TARGET_NAME} ${USEARCH_SYSTEM_INCLUDE} INTERFACE $<BUILD_INTERFACE:${USEARCH_INCLUDE_BUILD_DIR}>
                                                               $<INSTALL_INTERFACE:include>
)
if (USEARCH_USE_FP16LIB)
    target_include_directories(
        ${USEARCH_TARGET_NAME} ${USEARCH_SYSTEM_INCLUDE} INTERFACE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/fp16/include>
                                                                   $<INSTALL_INTERFACE:fp16/include>
    )
endif ()
if (USEARCH_USE_SIMSIMD)
    target_include_directories(
        ${USEARCH_TARGET_NAME} ${USEARCH_SYSTEM_INCLUDE}
        INTERFACE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/simsimd/include> $<INSTALL_INTERFACE:simsimd/include>
    )
endif ()

# Install a pkg-config file, so other tools can find this
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/pkg-config.pc.in" "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.pc")

# Install a custom package version config file instead of write_basic_package_version_file to ensure that it's
# architecture-independent https://github.com/nlohmann/json/issues/1697
include(CMakePackageConfigHelpers)
configure_file("cmake/usearchConfigVersion.cmake.in" ${USEARCH_CMAKE_VERSION_CONFIG_FILE} @ONLY)
configure_file(${USEARCH_CMAKE_CONFIG_TEMPLATE} ${USEARCH_CMAKE_PROJECT_CONFIG_FILE} @ONLY)

if (USEARCH_INSTALL)
    install(DIRECTORY ${USEARCH_INCLUDE_BUILD_DIR} DESTINATION ${USEARCH_INCLUDE_INSTALL_DIR})
    install(FILES ${USEARCH_CMAKE_PROJECT_CONFIG_FILE} ${USEARCH_CMAKE_VERSION_CONFIG_FILE}
            DESTINATION ${USEARCH_CONFIG_INSTALL_DIR}
    )
    export(
        TARGETS ${USEARCH_TARGET_NAME}
        NAMESPACE ${PROJECT_NAME}::
        FILE ${USEARCH_CMAKE_PROJECT_TARGETS_FILE}
    )
    install(
        TARGETS ${USEARCH_TARGET_NAME}
        EXPORT ${USEARCH_TARGETS_EXPORT_NAME}
        INCLUDES
        DESTINATION ${USEARCH_INCLUDE_INSTALL_DIR}
    )
    install(
        EXPORT ${USEARCH_TARGETS_EXPORT_NAME}
        NAMESPACE ${PROJECT_NAME}::
        DESTINATION ${USEARCH_CONFIG_INSTALL_DIR}
    )
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.pc" DESTINATION ${USEARCH_PKGCONFIG_INSTALL_DIR})
endif ()

# Find required packages
if (CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
    set(CMAKE_THREAD_LIBS_INIT "-lpthread")
    set(CMAKE_HAVE_THREADS_LIBRARY 1)
    set(CMAKE_USE_WIN32_THREADS_INIT 0)
    set(CMAKE_USE_PTHREADS_INIT 1)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
endif ()

find_package(Threads REQUIRED)

if (USEARCH_USE_OPENMP)
    find_package(OpenMP REQUIRED)
endif ()

if (USEARCH_USE_JEMALLOC)
    include(jemalloc)
endif ()

# Set default build type to Release
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

# Include directories
set(USEARCH_HEADER_INCLUDES "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_SOURCE_DIR}/fp16/include"
                            "${CMAKE_CURRENT_SOURCE_DIR}/simsimd/include"
)

# Function to setup target
function (setup_target TARGET_NAME)
    # Compiler-specific options List of all possible compiler IDs:
    # https://cmake.org/cmake/help/latest/variable/CMAKE_LANG_COMPILER_ID.html
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(
            ${TARGET_NAME}
            PRIVATE $<$<CONFIG:RELEASE>:-O3>
                    $<$<CONFIG:RELWITHDEBINFO>:-O3
                    -g>
                    $<$<CONFIG:DEBUG>:-O0
                    -g
                    -fsanitize=address
                    -fsanitize=leak
                    -fsanitize=alignment
                    -fsanitize=undefined
                    >
                    -ffast-math
                    -fPIC
                    -Wall
                    -Wextra
                    -Wno-conversion
                    -Wno-unknown-pragmas
                    -march=native
                    -fmax-errors=1
                    -pedantic
                    -fdiagnostics-color=always
        )
        target_link_options(
            ${TARGET_NAME}
            PRIVATE
            $<$<CONFIG:DEBUG>:-g
            -fsanitize=address
            -fsanitize=leak
            -fsanitize=alignment
            -fsanitize=undefined
            >
            -fPIC
        )

        if (USEARCH_USE_OPENMP)
            target_link_libraries(${TARGET_NAME} PRIVATE OpenMP::OpenMP_CXX)
        endif ()

        target_link_libraries(${TARGET_NAME} PRIVATE Threads::Threads)

    elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_compile_options(
            ${TARGET_NAME}
            PRIVATE $<$<CONFIG:RELEASE>:-O3>
                    $<$<CONFIG:RELWITHDEBINFO>:-O3
                    -g>
                    $<$<CONFIG:DEBUG>:-O0
                    -g
                    -fsanitize=address
                    -fsanitize=alignment
                    -fsanitize=undefined
                    >
                    -ffast-math
                    -fPIC
                    -pedantic
                    -Wfatal-errors
                    -fcolor-diagnostics
        )
        target_link_options(
            ${TARGET_NAME}
            PRIVATE
            $<$<CONFIG:DEBUG>:-g
            -fsanitize=address
            -fsanitize=alignment
            -fsanitize=undefined
            >
            -fPIC
        )

        # Check if the compiler is AppleClang, and if not, add the leak sanitizer
        if (CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
            # It's likely AppleClang Adjust options as needed for AppleClang
        else ()
            # It's likely LLVM Clang
            target_compile_options(${TARGET_NAME} PRIVATE $<$<CONFIG:DEBUG>:-fsanitize=leak>)
            target_link_options(${TARGET_NAME} PRIVATE $<$<CONFIG:DEBUG>:-fsanitize=leak>)
        endif ()

        if (USEARCH_USE_OPENMP)
            target_link_libraries(${TARGET_NAME} PRIVATE OpenMP::OpenMP_CXX)
        endif ()

        if (CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
            target_link_libraries(
                ${TARGET_NAME} PRIVATE "-framework CoreFoundation" "-framework Security" Threads::Threads
            )
        else ()
            target_link_libraries(${TARGET_NAME} PRIVATE Threads::Threads)
        endif ()

    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "NVIDIA" OR CMAKE_CXX_COMPILER_ID STREQUAL "NVHPC")
        target_compile_options(${TARGET_NAME} PRIVATE $<$<CONFIG:RELEASE>:-O3>)
        target_compile_definitions(${TARGET_NAME} PRIVATE --expt-relaxed-constexpr --extended-lambda)
        set_property(SOURCE bench.cpp PROPERTY LANGUAGE CUDA)
        set_target_properties(${TARGET_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)
        set_target_properties(${TARGET_NAME} PROPERTIES CUDA_ARCHITECTURES "86")

    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "IntelLLVM")
        target_compile_options(${TARGET_NAME} PRIVATE -w -ferror-limit=1)

    elseif (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
        # MSVC has no O3 optimization level
        target_compile_options(${TARGET_NAME} PRIVATE /wd4068 /wd4996)
        target_compile_options(
            ${TARGET_NAME} PRIVATE $<$<CONFIG:DEBUG>:/DEBUG> $<$<CONFIG:RELEASE>:/O2> $<$<CONFIG:RELWITHDEBINFO>:/O2
                                   /DEBUG>
        )
    endif ()

    target_include_directories(${TARGET_NAME} PRIVATE ${USEARCH_HEADER_INCLUDES})
    set_target_properties(${TARGET_NAME} PROPERTIES CXX_STANDARD 17)

    # On Windows, when using multi-configuration CMake builds, the paths will be nested, resulting in
    # "build_debug\Debug" on Windows instead of "build_debug". That's why we override all the configs to use the same
    # output directory.
    set_target_properties(
        ${TARGET_NAME}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
                   ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
                   LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
    )
    set_target_properties(
        ${TARGET_NAME}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}"
                   ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}"
                   LIBRARY_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}"
    )
    set_target_properties(
        ${TARGET_NAME}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}"
                   ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}"
                   LIBRARY_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}"
    )
    set_target_properties(
        ${TARGET_NAME}
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO "${CMAKE_BINARY_DIR}"
                   ARCHIVE_OUTPUT_DIRECTORY_RELWITHDEBINFO "${CMAKE_BINARY_DIR}"
                   LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO "${CMAKE_BINARY_DIR}"
    )

    # On Linux and MacOS our primary shared libraries for C are called `libusearch_c.so` and `libusearch_c.dylib`, while
    # on Windows the "lib" prefix might be missing by default, so let's ensure it's always there
    get_target_property(TARGET_TYPE ${TARGET_NAME} TYPE)

    if (TARGET_TYPE STREQUAL "SHARED_LIBRARY" OR TARGET_TYPE STREQUAL "STATIC_LIBRARY")
        set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "lib")
    endif ()

    # Core compilation settings affecting "index.hpp"
    target_compile_definitions(${TARGET_NAME} PRIVATE "USEARCH_USE_OPENMP=$<BOOL:${USEARCH_USE_OPENMP}>")

    # Supplementary compilation settings affecting "index_plugins.hpp"
    target_compile_definitions(${TARGET_NAME} PRIVATE "USEARCH_USE_FP16LIB=$<BOOL:${USEARCH_USE_FP16LIB}>")
    target_compile_definitions(${TARGET_NAME} PRIVATE "USEARCH_USE_SIMSIMD=$<BOOL:${USEARCH_USE_SIMSIMD}>")

    # Pass through SIMSIMD target definitions if they're set
    foreach(target IN ITEMS HASWELL SKYLAKE ICE GENOA SAPPHIRE TURIN SIERRA NEON NEON_I8 NEON_F16 NEON_BF16 SVE SVE_I8 SVE_F16 SVE_BF16 SVE2)
        if(DEFINED SIMSIMD_TARGET_${target})
            target_compile_definitions(${TARGET_NAME} PRIVATE "SIMSIMD_TARGET_${target}=${SIMSIMD_TARGET_${target}}")
        endif()
    endforeach()

    # Pass through SIMSIMD_DYNAMIC_DISPATCH if set
    if(DEFINED SIMSIMD_DYNAMIC_DISPATCH)
        target_compile_definitions(${TARGET_NAME} PRIVATE "SIMSIMD_DYNAMIC_DISPATCH=${SIMSIMD_DYNAMIC_DISPATCH}")
    endif()

endfunction ()

# Must be called before "add_subdirectory()". See
# https://stackoverflow.com/questions/30250494/ctest-not-detecting-tests.
enable_testing()

if (${USEARCH_BUILD_TEST_CPP} OR ${USEARCH_BUILD_BENCH_CPP})
    add_subdirectory(cpp)
endif ()

if (${USEARCH_BUILD_WOLFRAM})
    add_subdirectory(wolfram)
endif ()

if (${USEARCH_BUILD_WASM})
    add_subdirectory(wasm)
endif ()

if (${USEARCH_BUILD_LIB_C} OR ${USEARCH_BUILD_TEST_C})
    add_subdirectory(c)
endif ()

if (${USEARCH_BUILD_JNI})
    add_subdirectory(java)
endif ()

if (${USEARCH_BUILD_SQLITE})
    add_subdirectory(sqlite)
endif ()
