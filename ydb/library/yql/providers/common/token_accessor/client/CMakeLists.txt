add_library(common-token_accessor-client)
target_link_libraries(common-token_accessor-client PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-client
  cpp-threading-future
  providers-common-structured_token
  common-token_accessor-grpc
  client-ydb_types-credentials
)
target_sources(common-token_accessor-client PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/token_accessor/client/bearer_credentials_provider.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/token_accessor/client/factory.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/token_accessor/client/token_accessor_client.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/token_accessor/client/token_accessor_client_factory.cpp
)
