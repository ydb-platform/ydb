_ydb_sdk_add_library(client-ydb_datastreams)

target_link_libraries(client-ydb_datastreams PUBLIC
  yutil
  grpc-client
  string_utils-url
  api-grpc-draft
  library-operation_id
  impl-ydb_internal-make_request
  client-ydb_driver
)

target_sources(client-ydb_datastreams PRIVATE
  datastreams.cpp
)

_ydb_sdk_make_client_component(Datastreams client-ydb_datastreams)
