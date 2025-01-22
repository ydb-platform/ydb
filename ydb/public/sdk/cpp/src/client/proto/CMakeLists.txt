_ydb_sdk_add_library(client-ydb_proto)

target_link_libraries(client-ydb_proto PUBLIC
  yutil
  api-grpc
  api-grpc-draft
  api-protos
  library-operation_id
  client-ydb_params
  client-ydb_value
)

target_sources(client-ydb_proto PRIVATE
  accessor.cpp
)

_ydb_sdk_make_client_component(Proto client-ydb_proto)
