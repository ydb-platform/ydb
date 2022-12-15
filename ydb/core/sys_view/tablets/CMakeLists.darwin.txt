
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-sys_view-tablets)
target_compile_options(core-sys_view-tablets PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-sys_view-tablets PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-kqp-runtime
  core-sys_view-common
)
target_sources(core-sys_view-tablets PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/tablets/tablets.cpp
)
