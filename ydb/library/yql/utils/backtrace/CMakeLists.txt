add_library(yql-utils-backtrace)
target_link_libraries(yql-utils-backtrace PUBLIC
  contrib-libs-cxxsupp
  yutil
  lib-DebugInfo-Symbolize
)
target_sources(yql-utils-backtrace PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/backtrace/backtrace.cpp
)
