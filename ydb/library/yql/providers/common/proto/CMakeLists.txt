add_library(providers-common-proto)
target_link_libraries(providers-common-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-protos
  contrib-libs-protobuf
)
target_proto_messages(providers-common-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/proto/gateways_config.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/proto/udf_resolver.proto
)
target_proto_plugin(providers-common-proto
  cpp_styleguide
  cpp_styleguide
)
