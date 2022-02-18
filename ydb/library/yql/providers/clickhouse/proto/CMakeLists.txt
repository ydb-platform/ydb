add_library(providers-clickhouse-proto)
target_link_libraries(providers-clickhouse-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(providers-clickhouse-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/clickhouse/proto/source.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/clickhouse/proto/range.proto
)
target_proto_plugin(providers-clickhouse-proto
  cpp_styleguide
  cpp_styleguide
)
