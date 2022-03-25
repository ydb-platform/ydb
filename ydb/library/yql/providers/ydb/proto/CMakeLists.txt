
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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
target_sources(providers-ydb-proto PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/ydb/proto/range.pb.cc
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/ydb/proto/source.pb.cc
)
