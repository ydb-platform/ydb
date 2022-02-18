add_library(ydb-core-debug)
target_link_libraries(ydb-core-debug PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(ydb-core-debug PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/debug/valgrind_check.cpp
)
