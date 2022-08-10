
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yq-libs-init)
target_compile_options(yq-libs-init PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-init PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-http
  ydb-core-base
  ydb-core-protos
  yq-libs-actors
  yq-libs-audit
  yq-libs-checkpoint_storage
  yq-libs-checkpointing
  yq-libs-common
  yq-libs-control_plane_proxy
  yq-libs-control_plane_storage
  yq-libs-events
  yq-libs-gateway
  yq-libs-health
  yq-libs-quota_manager
  libs-rate_limiter-control_plane_service
  libs-rate_limiter-quoter_service
  yq-libs-shared_resources
  yq-libs-test_connection
  ydb-library-folder_service
  library-folder_service-proto
  yql-minikql-comp_nodes
  yql-utils-actor_log
  dq-actors-compute
  yql-dq-comp_nodes
  yql-dq-transform
  providers-clickhouse-actors
  providers-common-comp_nodes
  providers-common-metrics
  providers-dq-actors
  dq-api-protos
  providers-dq-provider
  providers-dq-task_runner
  providers-dq-worker_manager
  dq-worker_manager-interface
  providers-pq-async_io
  providers-pq-cm_client
  pq-gateway-native
  providers-pq-provider
  providers-s3-actors
  providers-s3-proto
  providers-s3-provider
  providers-solomon-async_io
  providers-solomon-gateway
  providers-solomon-proto
  providers-solomon-provider
  providers-ydb-actors
  providers-ydb-comp_nodes
  providers-ydb-provider
)
target_sources(yq-libs-init PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/init/init.cpp
)
