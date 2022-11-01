
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(public-lib-experimental)
target_link_libraries(public-lib-experimental PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-scheme
  api-grpc-draft
  cpp-client-ydb_proto
  cpp-client-ydb_table
)
target_sources(public-lib-experimental PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/experimental/ydb_clickhouse_internal.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/experimental/ydb_logstore.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/experimental/ydb_s3_internal.cpp
)
