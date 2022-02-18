add_library(yq-libs-read_rule)
target_compile_options(yq-libs-read_rule PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-read_rule PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-protos
  yq-libs-events
  api-protos
  lib-operation_id-protos
  cpp-client-ydb_persqueue_core
  providers-common-proto
  dq-api-protos
  providers-pq-proto
)
target_sources(yq-libs-read_rule PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/read_rule/read_rule_creator.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/read_rule/read_rule_deleter.cpp
)
