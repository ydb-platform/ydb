
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(ydb-core-blob_depot)
target_link_libraries(ydb-core-blob_depot PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-tablet_flat
  ydb-core-protos
)
target_sources(ydb-core-blob_depot PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/blob_depot.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/blob_depot_agent.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/blocks.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/garbage_collection.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/op_apply_config.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/op_init_schema.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/op_load.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/op_resolve.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blob_depot/op_commit_blob_seq.cpp
)
