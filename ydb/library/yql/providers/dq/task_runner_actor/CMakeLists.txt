add_library(providers-dq-task_runner_actor)
target_compile_options(providers-dq-task_runner_actor PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-task_runner_actor PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  dq-actors-task_runner
  dq-api-protos
  yql-utils-actors
  providers-dq-task_runner
)
target_sources(providers-dq-task_runner_actor PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/task_runner_actor/task_runner_actor.cpp
)
