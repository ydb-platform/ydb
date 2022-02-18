add_library(cpp-client-ydb_datastreams)
target_link_libraries(cpp-client-ydb_datastreams PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-client
  cpp-string_utils-url
  api-grpc-draft
  public-lib-operation_id
  impl-ydb_internal-make_request
  cpp-client-ydb_driver
)
target_sources(cpp-client-ydb_datastreams PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.cpp
)
