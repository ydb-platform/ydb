add_library(providers-dq-worker_manager)
target_compile_options(providers-dq-worker_manager PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-worker_manager PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-kqp
  yql-utils-failure_injector
  providers-common-config
  providers-common-gateway
  providers-common-metrics
  dq-api-grpc
  dq-api-protos
  providers-dq-runtime
  providers-dq-task_runner
  providers-dq-task_runner_actor
  dq-worker_manager-interface
)
target_sources(providers-dq-worker_manager PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/worker_manager/local_worker_manager.cpp
)
