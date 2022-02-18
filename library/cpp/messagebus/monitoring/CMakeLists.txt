add_library(cpp-messagebus-monitoring)
target_link_libraries(cpp-messagebus-monitoring PUBLIC
  contrib-libs-cxxsupp
  yutil
  encode-legacy_protobuf-protos
  contrib-libs-protobuf
)
target_proto_messages(cpp-messagebus-monitoring PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/monitoring/mon_proto.proto
)
target_proto_plugin(cpp-messagebus-monitoring
  cpp_styleguide
  cpp_styleguide
)
