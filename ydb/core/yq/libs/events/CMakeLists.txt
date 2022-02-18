add_library(yq-libs-events)
target_compile_options(yq-libs-events PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-events PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  cpp-actors-core
  libs-graph_params-proto
  yql-core-facade
  yql-public-issue
  api-protos
  public-lib-yq
  cpp-client-ydb_table
  providers-common-db_id_async_resolver
  providers-dq-provider
)
generate_enum_serilization(yq-libs-events
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/events/events.h
  INCLUDE_HEADERS
  ydb/core/yq/libs/events/events.h
)
generate_enum_serilization(yq-libs-events
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/events/event_ids.h
  INCLUDE_HEADERS
  ydb/core/yq/libs/events/event_ids.h
)
