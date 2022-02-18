add_library(libs-checkpoint_storage-proto)
target_link_libraries(libs-checkpoint_storage-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-graph_params-proto
  contrib-libs-protobuf
)
target_proto_messages(libs-checkpoint_storage-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpoint_storage/proto/graph_description.proto
)
target_proto_plugin(libs-checkpoint_storage-proto
  cpp_styleguide
  cpp_styleguide
)
