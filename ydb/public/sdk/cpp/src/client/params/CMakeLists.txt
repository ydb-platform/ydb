_ydb_sdk_add_library(client-ydb_params)

target_link_libraries(client-ydb_params PUBLIC
  yutil
  api-protos
  client-ydb_types-fatal_error_handlers
  client-ydb_value
)

target_sources(client-ydb_params PRIVATE
  params.cpp
  impl.cpp
)

_ydb_sdk_make_client_component(Params client-ydb_params)
