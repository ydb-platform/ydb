
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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
target_sources(library-yql-protos PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/protos/common.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/library/yql/protos/yql_mount.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/library/yql/protos/clickhouse.pb.cc
)
