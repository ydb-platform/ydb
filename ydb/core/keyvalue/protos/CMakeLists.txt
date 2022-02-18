add_library(core-keyvalue-protos)
target_link_libraries(core-keyvalue-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(core-keyvalue-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/keyvalue/protos/events.proto
)
target_proto_plugin(core-keyvalue-protos
  cpp_styleguide
  cpp_styleguide
)
