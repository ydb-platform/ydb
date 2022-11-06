
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(events)

add_library(yq-libs-control_plane_config)
target_compile_options(yq-libs-control_plane_config PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-control_plane_config PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-lwtrace-mon
  monlib-service-pages
  ydb-core-base
  ydb-core-mon
  yq-libs-common
  yq-libs-config
  libs-control_plane_config-events
  yq-libs-quota_manager
  libs-quota_manager-events
  libs-rate_limiter-events
  yq-libs-ydb
  ydb-library-security
  cpp-client-ydb_scheme
  cpp-client-ydb_value
  ydb-library-protobuf_printer
  yql-public-issue
)
target_sources(yq-libs-control_plane_config PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/control_plane_config/control_plane_config.cpp
)
