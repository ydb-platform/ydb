add_library(providers-pq-task_meta)
target_compile_options(providers-pq-task_meta PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-pq-task_meta PUBLIC
  contrib-libs-cxxsupp
  yutil
  dq-api-protos
  providers-pq-proto
)
target_sources(providers-pq-task_meta PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/task_meta/task_meta.cpp
)
