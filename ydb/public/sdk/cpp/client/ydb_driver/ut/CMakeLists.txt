add_executable(ydb-public-sdk-cpp-client-ydb_driver-ut)
target_include_directories(ydb-public-sdk-cpp-client-ydb_driver-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_driver
)
target_link_libraries(ydb-public-sdk-cpp-client-ydb_driver-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-client-ydb_driver
  cpp-client-ydb_table
)
target_sources(ydb-public-sdk-cpp-client-ydb_driver-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_driver/driver_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-ydb_driver-ut
  COMMAND
  ydb-public-sdk-cpp-client-ydb_driver-ut
)
target_link_flags(ydb-public-sdk-cpp-client-ydb_driver-ut
  PUBLIC
  -ldl
  -lrt
  -Wl,--no-as-needed
  -Wl,-rpath,$ORIGIN
  -fPIC
  -Wl,--gdb-index
  -Wl,-rpath,$ORIGIN
  -fPIC
  -fuse-ld=$(LLD_ROOT-sbr:2283360772)/ld
  -Wl,--no-rosegment
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-public-sdk-cpp-client-ydb_driver-ut)
