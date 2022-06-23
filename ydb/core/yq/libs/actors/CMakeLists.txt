
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yq-libs-actors)
target_compile_options(yq-libs-actors PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-interconnect
  cpp-json-yson
  cpp-monlib-dynamic_counters
  library-cpp-random_provider
  library-cpp-time_provider
  library-cpp-yson
  cpp-yson-node
  ydb-core-base
  ydb-core-protos
  libs-actors-logging
  yq-libs-checkpointing
  yq-libs-checkpointing_common
  yq-libs-db_id_async_resolver_impl
  yq-libs-common
  yq-libs-control_plane_storage
  libs-control_plane_storage-events
  yq-libs-db_schema
  yq-libs-events
  yq-libs-private_client
  yq-libs-result_formatter
  yq-libs-shared_resources
  yq-libs-signer
  ydb-library-mkql_proto
  ydb-library-security
  library-yql-ast
  yql-core-facade
  core-services-mounts
  library-yql-minikql
  yql-minikql-comp_nodes
  common-token_accessor-client
  yql-public-issue
  public-issue-protos
  yql-sql-settings
  yql-utils-actor_log
  api-protos
  public-lib-yq
  cpp-client-ydb_table
  providers-clickhouse-provider
  providers-common-codec
  providers-common-comp_nodes
  providers-common-db_id_async_resolver
  providers-common-metrics
  providers-common-provider
  common-schema-mkql
  providers-common-udf_resolve
  providers-dq-actors
  providers-dq-common
  providers-dq-counters
  providers-dq-interface
  providers-dq-provider
  dq-provider-exec
  dq-worker_manager-interface
  providers-pq-cm_client
  providers-pq-provider
  providers-pq-task_meta
  providers-s3-provider
  providers-ydb-provider
  library-yql-utils
)
target_sources(yq-libs-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/clusters_from_connections.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/database_resolver.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/error.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/nodes_health_check.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/nodes_manager.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/pending_fetcher.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/pinger.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/proxy.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/proxy_private.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/result_writer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/run_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/run_actor_params.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/system_clusters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/table_bindings_from_bindings.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/task_get.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/task_ping.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/actors/task_result_write.cpp
)
