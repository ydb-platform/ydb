add_library(cpp-client-ydb_discovery)
target_link_libraries(cpp-client-ydb_discovery PUBLIC
  contrib-libs-cxxsupp
  yutil
  client-ydb_common_client-impl
  cpp-client-ydb_driver
)
target_sources(cpp-client-ydb_discovery PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_discovery/discovery.cpp
)
