add_library(protobuf-util-proto)
target_link_libraries(protobuf-util-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(protobuf-util-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/protobuf/util/proto/merge.proto
)
target_proto_plugin(protobuf-util-proto
  cpp_styleguide
  cpp_styleguide
)
