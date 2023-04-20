cmake_minimum_required(VERSION 3.10)
project(ydb_capnp_tests)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)

# Locate Google Test
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Include the necessary headers from the YDB codebase and Cap'n Proto
include_directories(${CMAKE_SOURCE_DIR})

# Add your test source file
set(TEST_SOURCES tevvget_test.cpp)

# Create the test executable and link it with the necessary libraries
add_executable(ydb_capnp_tests ${TEST_SOURCES})
target_link_libraries(ydb_capnp_tests ${GTEST_BOTH_LIBRARIES} blobstorage-vdisk-common)

# Enable testing and add your test executable as a test
enable_testing()
gtest_discover_tests(ydb_capnp_tests)
