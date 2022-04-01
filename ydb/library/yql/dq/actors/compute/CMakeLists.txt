
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(dq-actors-compute)
target_compile_options(dq-actors-compute PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(dq-actors-compute PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  ydb-core-scheme
  ydb-core-tablet_flat
  yql-dq-common
  yql-dq-proto
  yql-dq-runtime
  yql-dq-tasks
  yql-minikql-comp_nodes
  yql-minikql-computation
  yql-public-issue
)
target_sources(dq-actors-compute PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/dq_compute_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/dq_async_compute_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/dq_compute_actor_channels.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/dq_compute_actor_stats.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/dq_compute_issues_buffer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/compute/retry_queue.cpp
)
