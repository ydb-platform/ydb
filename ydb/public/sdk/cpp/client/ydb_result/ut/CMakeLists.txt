add_executable(ydb-public-sdk-cpp-client-ydb_result-ut)
target_include_directories(ydb-public-sdk-cpp-client-ydb_result-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_result
)
target_link_libraries(ydb-public-sdk-cpp-client-ydb_result-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-client-ydb_result
)
target_sources(ydb-public-sdk-cpp-client-ydb_result-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_result/result_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-ydb_result-ut
  COMMAND
  ydb-public-sdk-cpp-client-ydb_result-ut
)
target_link_flags(ydb-public-sdk-cpp-client-ydb_result-ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-public-sdk-cpp-client-ydb_result-ut)
