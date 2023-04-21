cmake_minimum_required(VERSION 3.10)

# Create a new test executable
add_executable(ydb_capnp_tests tevvget_test.cpp)

# Add GoogleTest
enable_testing()
find_package(GTest REQUIRED)

# Link required libraries
target_link_libraries(ydb_capnp_tests PUBLIC
        contrib-libs-linux-headers
        contrib-libs-cxxsupp
        yutil
        blobstorage-vdisk-common
        cpp-testing-unittest
        cpp-testing-unittest_main
        capnp
        kj
        ${GTEST_LIBRARIES}
        )

target_link_directories(ydb_capnp_tests PUBLIC /usr/local/lib)
target_include_directories(ydb_capnp_tests PRIVATE ${GTEST_INCLUDE_DIRS})

find_path(CAPNP_INCLUDE_DIR capnp/message.h)
target_include_directories(ydb_capnp_tests PRIVATE ${CAPNP_INCLUDE_DIR})

# Add the test to CTest
include(GoogleTest)
gtest_discover_tests(ydb_capnp_tests
        TEST_PREFIX "ydb_capnp_tests."
        DISCOVERY_TIMEOUT 120
        )
