add_executable(ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut)
target_include_directories(ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_endpoints
)
target_link_libraries(ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  client-impl-ydb_endpoints
)
target_sources(ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_endpoints/endpoints_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut
  COMMAND
  ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut
)
target_link_flags(ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut
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
vcs_info(ydb-public-sdk-cpp-client-impl-ydb_endpoints-ut)
