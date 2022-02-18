add_library(validation-ut-protos)
target_link_libraries(validation-ut-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos-annotations
  contrib-libs-protobuf
)
target_proto_messages(validation-ut-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/validation/ut/protos/validation_test.proto
)
target_proto_plugin(validation-ut-protos
  cpp_styleguide
  cpp_styleguide
)
target_proto_plugin(validation-ut-protos
  validation
  validation
)
