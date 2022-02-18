add_library(cpp-client-ydb_common_client)
target_link_libraries(cpp-client-ydb_common_client PUBLIC
  contrib-libs-cxxsupp
  yutil
  impl-ydb_internal-common
  client-ydb_types-credentials
)
target_sources(cpp-client-ydb_common_client PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_common_client/settings.cpp
)
