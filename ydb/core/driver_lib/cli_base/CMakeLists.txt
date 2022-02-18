add_library(cli_base)
target_compile_options(cli_base PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(cli_base PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-driver_lib-cli_config_base
  ydb-library-aclib
  lib-deprecated-kicli
  common
  cpp-client-resources
  cpp-client-ydb_table
)
target_sources(cli_base PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/cli_base/cli_cmds_db.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/cli_base/cli_cmds_discovery.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/cli_base/cli_cmds_root.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/cli_base/cli_cmds_whoami.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/cli_base/cli_kicli.cpp
)
