add_executable(ydb-public-sdk-cpp-client-ydb_params-ut)
target_include_directories(ydb-public-sdk-cpp-client-ydb_params-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_params
)
target_link_libraries(ydb-public-sdk-cpp-client-ydb_params-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-client-ydb_params
  public-lib-yson_value
)
target_sources(ydb-public-sdk-cpp-client-ydb_params-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_params/params_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-ydb_params-ut
  COMMAND
  ydb-public-sdk-cpp-client-ydb_params-ut
)
target_link_flags(ydb-public-sdk-cpp-client-ydb_params-ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-public-sdk-cpp-client-ydb_params-ut)
