
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-driver_lib-cli_config_base)
target_compile_options(core-driver_lib-cli_config_base PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-driver_lib-cli_config_base PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-enum_codegen
  ydb-core-util
  lib-deprecated-client
  library-yql-minikql
)
target_sources(core-driver_lib-cli_config_base PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/driver_lib/cli_config_base/config_base.cpp
)
