
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(clicommands)
target_link_libraries(clicommands PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-histogram-hdr
  cpp-protobuf-json
  cpp-regex-pcre
  cpp-threading-local_executor
  kikimr_backup
  ydb-library-workload
  public-lib-operation_id
  common
  lib-ydb_cli-dump
  lib-ydb_cli-import
  cpp-client-draft
  cpp-client-ydb_discovery
  cpp-client-ydb_export
  cpp-client-ydb_import
  cpp-client-ydb_operation
  cpp-client-ydb_persqueue_public
  cpp-client-ydb_proto
  cpp-client-ydb_scheme
  cpp-client-ydb_table
  ydb_types-credentials-login
)
target_sources(clicommands PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/stock_workload.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_command.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_profile.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_root_common.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_discovery.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_export.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_import.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_operation.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_scheme.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_scripting.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_topic.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_service_table.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_tools.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_workload.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/ydb_yql.cpp
)
