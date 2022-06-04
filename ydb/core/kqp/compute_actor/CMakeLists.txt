
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-kqp-compute_actor)
target_compile_options(core-kqp-compute_actor PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-compute_actor PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-actorlib_impl
  ydb-core-base
  core-kqp-runtime
  core-tx-datashard
  core-tx-scheme_cache
  dq-actors-compute
  yql-public-issue
)
target_sources(core-kqp-compute_actor PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/compute_actor/kqp_compute_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/compute_actor/kqp_compute_actor_helpers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/compute_actor/kqp_pure_compute_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/compute_actor/kqp_scan_compute_actor.cpp
)
