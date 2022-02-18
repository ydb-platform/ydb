add_library(providers-dq-backtrace)
target_compile_options(providers-dq-backtrace PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-backtrace PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-utils-backtrace
)
target_sources(providers-dq-backtrace PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/backtrace/backtrace.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/backtrace/symbolizer.cpp
)
