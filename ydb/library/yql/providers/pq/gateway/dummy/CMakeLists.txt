add_library(pq-gateway-dummy)
target_link_libraries(pq-gateway-dummy PUBLIC
  contrib-libs-cxxsupp
  yutil
  providers-pq-provider
)
target_sources(pq-gateway-dummy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/gateway/dummy/yql_pq_dummy_gateway.cpp
)
