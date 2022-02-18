add_library(yq-libs-gateway)
target_compile_options(yq-libs-gateway PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-gateway PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-monlib-dynamic_counters
  yq-libs-actors
  yq-libs-events
  yq-libs-read_rule
  yq-libs-shared_resources
  yq-libs-tasks_packer
  common-token_accessor-client
  yql-public-issue
  api-protos
  providers-common-metrics
  providers-dq-actors
  dq-api-protos
  providers-pq-proto
)
target_sources(yq-libs-gateway PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/gateway/empty_gateway.cpp
)
