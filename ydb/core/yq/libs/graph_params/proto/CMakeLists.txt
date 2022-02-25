add_library(libs-graph_params-proto)
target_link_libraries(libs-graph_params-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-dq-proto
  dq-api-protos
  contrib-libs-protobuf
)
target_proto_messages(libs-graph_params-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/graph_params/proto/graph_params.proto
)
