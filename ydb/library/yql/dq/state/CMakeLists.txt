add_library(yql-dq-state)
target_link_libraries(yql-dq-state PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-issue
  core-issue-protos
  yql-dq-proto
  providers-pq-proto
  providers-pq-task_meta
)
target_sources(yql-dq-state PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/state/dq_state_load_plan.cpp
)
