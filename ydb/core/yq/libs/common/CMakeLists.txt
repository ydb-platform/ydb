
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yq-libs-common)
target_compile_options(yq-libs-common PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-blockcodecs
  libs-control_plane_storage-events
  yq-libs-events
  providers-common-structured_token
  yql-public-issue
  api-protos
)
target_sources(yq-libs-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/common/compression.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/common/entity_id.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/common/rows_proto_splitter.cpp
)
