add_library(providers-dq-planner)
target_compile_options(providers-dq-planner PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-planner PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-core-services
  yql-minikql-comp_nodes
  yql-minikql-computation
  yql-dq-opt
  yql-dq-tasks
  providers-common-mkql
  dq-api-protos
  providers-dq-common
  providers-dq-expr_nodes
  providers-dq-interface
  providers-dq-mkql
  providers-dq-opt
)
target_sources(providers-dq-planner PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/planner/dqs_task_graph.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/planner/execution_planner.cpp
)
