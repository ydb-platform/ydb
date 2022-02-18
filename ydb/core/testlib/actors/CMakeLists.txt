add_library(core-testlib-actors)
target_link_libraries(core-testlib-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-testlib
  cpp-testing-unittest
  ydb-core-base
  ydb-core-mon
  ydb-core-mon_alloc
  ydb-core-scheme
  ydb-core-tablet
)
target_sources(core-testlib-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/testlib/actors/test_runtime.cpp
)
