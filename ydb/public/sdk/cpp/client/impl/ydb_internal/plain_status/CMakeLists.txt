add_library(impl-ydb_internal-plain_status)
target_link_libraries(impl-ydb_internal-plain_status PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-grpc-client
  yql-public-issue
)
target_sources(impl-ydb_internal-plain_status PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.cpp
)
