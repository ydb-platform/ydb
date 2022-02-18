add_library(ymq-queues-common)
target_link_libraries(ymq-queues-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-ymq-base
)
target_sources(ymq-queues-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/queues/common/queries.cpp
)
