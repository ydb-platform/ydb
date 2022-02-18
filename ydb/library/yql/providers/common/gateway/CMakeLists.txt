add_library(providers-common-gateway)
target_link_libraries(providers-common-gateway PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-future
  library-yql-ast
)
target_sources(providers-common-gateway PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/gateway/yql_provider_gateway.cpp
)
