add_executable(ydb-public-sdk-cpp-client-ydb_coordination-ut)
target_include_directories(ydb-public-sdk-cpp-client-ydb_coordination-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_coordination
)
target_link_libraries(ydb-public-sdk-cpp-client-ydb_coordination-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-client-ydb_coordination
  api-grpc
)
target_sources(ydb-public-sdk-cpp-client-ydb_coordination-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_coordination/coordination_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-ydb_coordination-ut
  COMMAND
  ydb-public-sdk-cpp-client-ydb_coordination-ut
)
target_link_flags(ydb-public-sdk-cpp-client-ydb_coordination-ut
  PUBLIC
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -Wl,--gdb-index
  -fPIC
  -fuse-ld=$(LLD_ROOT-sbr:2283360772)/ld
  -Wl,--no-rosegment
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-public-sdk-cpp-client-ydb_coordination-ut)
