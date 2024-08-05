_ydb_sdk_add_library(client-ydb_result)

target_link_libraries(client-ydb_result PUBLIC
  yutil
  api-protos
  client-ydb_types-fatal_error_handlers
  client-ydb_value
  client-ydb_proto
)

target_sources(client-ydb_result PRIVATE
  proto_accessor.cpp
  result.cpp
  out.cpp
)

_ydb_sdk_make_client_component(Result client-ydb_result)
