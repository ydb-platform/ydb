add_library(yq-libs-private_client)
target_link_libraries(yq-libs-private_client PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-dynamic_counters
  cpp-protobuf-json
  api-grpc-draft
  cpp-client-ydb_table
)
target_sources(yq-libs-private_client PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/private_client/utils.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/private_client/private_client.cpp
)
