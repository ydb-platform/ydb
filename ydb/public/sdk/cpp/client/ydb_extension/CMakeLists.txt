add_library(cpp-client-ydb_extension)
target_link_libraries(cpp-client-ydb_extension PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-metrics
  cpp-client-ydb_driver
)
target_sources(cpp-client-ydb_extension PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_extension/extension.cpp
)
