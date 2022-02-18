add_library(yql-dq-tasks)
target_compile_options(yql-dq-tasks PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-dq-tasks PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-core
  yql-dq-expr_nodes
  yql-dq-proto
)
target_sources(yql-dq-tasks PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/tasks/dq_task_program.cpp
)
