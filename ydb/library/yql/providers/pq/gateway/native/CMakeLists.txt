add_library(pq-gateway-native)
target_link_libraries(pq-gateway-native PUBLIC
  contrib-libs-cxxsupp
  yutil
  common-token_accessor-client
  library-yql-utils
  cpp-client-ydb_driver
  cpp-client-ydb_persqueue_core
  providers-common-metrics
  providers-common-proto
  pq-cm_client-interface
  providers-pq-provider
)
target_sources(pq-gateway-native PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/gateway/native/yql_pq_session.cpp
)
