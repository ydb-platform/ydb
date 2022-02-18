add_library(base_utils)
target_compile_options(base_utils PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(base_utils PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-enum_codegen
  cpp-grpc-client
  core-blobstorage-pdisk
  core-client-server
  core-driver_lib-cli_config_base
  ydb-core-protos
  lib-deprecated-client
)
target_sources(base_utils PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/base_utils/format_info.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/base_utils/format_util.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/base_utils/node_by_host.cpp
)
