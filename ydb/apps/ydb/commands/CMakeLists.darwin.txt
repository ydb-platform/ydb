
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(commands)
target_link_libraries(commands PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-client-iam
  clicommands
)
target_sources(commands PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/apps/ydb/commands/ydb_cloud_root.cpp
  ${CMAKE_SOURCE_DIR}/ydb/apps/ydb/commands/ydb_update.cpp
  ${CMAKE_SOURCE_DIR}/ydb/apps/ydb/commands/ydb_version.cpp
)
