add_executable(ydb-public-sdk-cpp-client-ydb_table-impl-ut)
target_include_directories(ydb-public-sdk-cpp-client-ydb_table-impl-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_table/impl
)
target_link_libraries(ydb-public-sdk-cpp-client-ydb_table-impl-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  client-ydb_table-impl
  cpp-client-ydb_table
)
target_sources(ydb-public-sdk-cpp-client-ydb_table-impl-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_table/impl/request_migrator_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-ydb_table-impl-ut
  COMMAND
  ydb-public-sdk-cpp-client-ydb_table-impl-ut
)
target_link_flags(ydb-public-sdk-cpp-client-ydb_table-impl-ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-public-sdk-cpp-client-ydb_table-impl-ut)
