add_library(public-lib-yson_value)
target_link_libraries(public-lib-yson_value PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-yson
  cpp-yson-node
  cpp-client-ydb_result
  cpp-client-ydb_value
)
target_sources(public-lib-yson_value PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/yson_value/ydb_yson_value.cpp
)
