add_library(ymq-queues-fifo)
target_link_libraries(ymq-queues-fifo PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-ymq-base
)
target_sources(ymq-queues-fifo PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/queues/fifo/queries.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/queues/fifo/schema.cpp
)
