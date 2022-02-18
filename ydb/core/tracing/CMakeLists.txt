add_library(ydb-core-tracing)
target_link_libraries(ydb-core-tracing PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
)
target_sources(ydb-core-tracing PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tracing/http.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tracing/tablet_info.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tracing/trace.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tracing/trace_collection.cpp
)
