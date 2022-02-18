add_library(providers-common-http_gateway)
target_compile_options(providers-common-http_gateway PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-common-http_gateway PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-curl
  cpp-monlib-dynamic_counters
  providers-common-proto
  yql-public-issue
)
target_sources(providers-common-http_gateway PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/http_gateway/yql_http_gateway.cpp
)
