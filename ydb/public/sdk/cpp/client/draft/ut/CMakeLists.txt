add_executable(ydb-public-sdk-cpp-client-draft-ut)
target_include_directories(ydb-public-sdk-cpp-client-draft-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/draft
)
target_link_libraries(ydb-public-sdk-cpp-client-draft-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-client-draft
)
target_sources(ydb-public-sdk-cpp-client-draft-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/draft/ydb_scripting_response_headers_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-draft-ut
  COMMAND
  ydb-public-sdk-cpp-client-draft-ut
)
target_link_flags(ydb-public-sdk-cpp-client-draft-ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-public-sdk-cpp-client-draft-ut)
