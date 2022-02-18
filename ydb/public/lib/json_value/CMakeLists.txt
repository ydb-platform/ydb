add_library(public-lib-json_value)
target_link_libraries(public-lib-json_value PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-json-writer
  cpp-string_utils-base64
  cpp-client-ydb_result
  cpp-client-ydb_value
)
target_sources(public-lib-json_value PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/json_value/ydb_json_value.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/json_value/ydb_json_value_ut.cpp
)
