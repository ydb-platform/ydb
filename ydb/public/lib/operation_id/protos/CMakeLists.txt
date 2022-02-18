add_library(lib-operation_id-protos)
target_link_libraries(lib-operation_id-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(lib-operation_id-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/operation_id/protos/operation_id.proto
)
target_proto_plugin(lib-operation_id-protos
  cpp_styleguide
  cpp_styleguide
)
