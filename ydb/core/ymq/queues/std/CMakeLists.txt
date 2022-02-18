add_library(ymq-queues-std)
target_link_libraries(ymq-queues-std PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-ymq-base
)
target_sources(ymq-queues-std PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/queues/std/queries.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/queues/std/schema.cpp
)
