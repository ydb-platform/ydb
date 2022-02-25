add_library(encode-legacy_protobuf-protos)
target_link_libraries(encode-legacy_protobuf-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(encode-legacy_protobuf-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/legacy_protobuf/protos/metric_meta.proto
)
