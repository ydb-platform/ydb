add_library(cpp-client-ydb_params)
target_link_libraries(cpp-client-ydb_params PUBLIC
  contrib-libs-cxxsupp
  yutil
  client-ydb_types-fatal_error_handlers
  cpp-client-ydb_value
)
target_sources(cpp-client-ydb_params PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_params/params.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_params/impl.cpp
)
