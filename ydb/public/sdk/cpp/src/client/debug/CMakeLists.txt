_ydb_sdk_add_library(client-debug)

target_link_libraries(client-debug
  PUBLIC
    client-ydb_driver 
  PRIVATE
    api-grpc
    api-protos
    client-ydb_common_client-impl
)

target_sources(client-debug
  PRIVATE
    client.cpp
)

_ydb_sdk_make_client_component(Debug client-debug)
