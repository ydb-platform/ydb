
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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
target_sources(providers-clickhouse-proto PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/clickhouse/proto/source.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/clickhouse/proto/range.pb.cc
)
