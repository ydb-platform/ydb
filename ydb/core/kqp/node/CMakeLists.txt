
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

add_library(core-kqp-node)
target_compile_options(core-kqp-node PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-node PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-actorlib_impl
  ydb-core-base
  core-cms-console
  core-kqp-common
  core-kqp-counters
  ydb-core-mind
  ydb-core-protos
  ydb-core-tablet
  dq-actors-compute
)
target_sources(core-kqp-node PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/node/kqp_node.cpp
)
