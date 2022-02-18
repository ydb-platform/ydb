add_library(providers-ydb-proto)
target_link_libraries(providers-ydb-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(providers-ydb-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/ydb/proto/range.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/ydb/proto/source.proto
)
target_proto_plugin(providers-ydb-proto
  cpp_styleguide
  cpp_styleguide
)
