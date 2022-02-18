add_library(libs-control_plane_storage-proto)
target_link_libraries(libs-control_plane_storage-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
  yql-dq-proto
  dq-api-protos
  contrib-libs-protobuf
)
target_proto_messages(libs-control_plane_storage-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/control_plane_storage/proto/yq_internal.proto
)
target_proto_plugin(libs-control_plane_storage-proto
  cpp_styleguide
  cpp_styleguide
)
