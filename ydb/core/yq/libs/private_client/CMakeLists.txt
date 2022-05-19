
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yq-libs-private_client)
target_compile_options(yq-libs-private_client PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-private_client PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-dynamic_counters
  cpp-protobuf-json
  api-grpc-draft
  cpp-client-ydb_table
  libs-control_plane_storage-proto
  yq-libs-shared_resources
  ydb-core-protos
)
target_sources(yq-libs-private_client PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/private_client/internal_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/private_client/loopback_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/private_client/private_client.cpp
)
