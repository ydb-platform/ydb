add_library(providers-solomon-gateway)
target_link_libraries(providers-solomon-gateway PUBLIC
  contrib-libs-cxxsupp
  yutil
  providers-common-gateway
  providers-solomon-provider
)
target_sources(providers-solomon-gateway PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/solomon/gateway/yql_solomon_gateway.cpp
)
