add_library(providers-s3-proto)
target_link_libraries(providers-s3-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(providers-s3-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/proto/range.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/proto/retry_config.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/proto/source.proto
)
target_proto_plugin(providers-s3-proto
  cpp_styleguide
  cpp_styleguide
)
