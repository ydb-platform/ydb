
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(events)
add_subdirectory(ut)

add_library(yq-libs-control_plane_proxy)
target_compile_options(yq-libs-control_plane_proxy PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-control_plane_proxy PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-mon
  libs-actors-logging
  yq-libs-actors
  yq-libs-control_plane_config
  libs-control_plane_proxy-events
  yq-libs-control_plane_storage
  libs-rate_limiter-events
  ydb-library-folder_service
  ydb-library-security
)
target_sources(yq-libs-control_plane_proxy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/control_plane_proxy/config.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/control_plane_proxy/control_plane_proxy.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/control_plane_proxy/probes.cpp
)
