add_library(providers-dq-counters)
target_compile_options(providers-dq-counters PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-counters PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-dq-runtime
)
target_sources(providers-dq-counters PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/counters/counters.cpp
)
