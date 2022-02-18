add_library(core-persqueue-partition_key_range)
target_link_libraries(core-persqueue-partition_key_range PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
  ydb-core-scheme
)
target_sources(core-persqueue-partition_key_range PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/partition_key_range/partition_key_range.cpp
)
