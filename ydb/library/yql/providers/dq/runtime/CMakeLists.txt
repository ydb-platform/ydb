add_library(providers-dq-runtime)
target_compile_options(providers-dq-runtime PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-runtime PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-minikql
  yql-minikql-computation
  yql-minikql-invoke_builtins
  library-yql-utils
  yql-utils-log
  yql-core-expr_nodes
  yql-dq-common
  yql-dq-runtime
  providers-common-proto
  dq-api-protos
  providers-dq-backtrace
  providers-dq-common
  providers-dq-counters
  providers-dq-interface
  providers-dq-task_runner
)
target_sources(providers-dq-runtime PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/runtime/file_cache.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/runtime/task_command_executor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/runtime/runtime_data.cpp
)
