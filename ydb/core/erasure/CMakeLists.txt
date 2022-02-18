add_library(ydb-core-erasure)
target_link_libraries(ydb-core-erasure PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-util
  cpp-containers-stack_vector
  cpp-digest-crc32c
  cpp-digest-old_crc
  ydb-core-debug
)
target_sources(ydb-core-erasure PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/erasure/erasure.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/erasure/erasure_rope.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/erasure/erasure_perf_test.cpp
)
