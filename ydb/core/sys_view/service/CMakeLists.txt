
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

add_library(core-sys_view-service)
target_link_libraries(core-sys_view-service PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  library-aclib-protos
)
target_sources(core-sys_view-service PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/db_counters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/ext_counters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/query_interval.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/sysview_service.cpp
)
