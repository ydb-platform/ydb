add_library(library-yql-protos)
target_link_libraries(library-yql-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(library-yql-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/protos/common.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/protos/yql_mount.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/protos/clickhouse.proto
)
target_proto_plugin(library-yql-protos
  cpp_styleguide
  cpp_styleguide
)
