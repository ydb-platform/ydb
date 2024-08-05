_ydb_sdk_add_library(client-bsconfig)

target_link_libraries(client-bsconfig
  PUBLIC
    client-ydb_driver
    client-ydb_common_client
    client-ydb_types-status
    client-ydb_types
  PRIVATE
    api-grpc
    impl-ydb_internal-make_request
    client-ydb_common_client-impl
)

target_sources(client-bsconfig
  PRIVATE
    storage_config.cpp
)

_ydb_sdk_make_client_component(BSConfig client-bsconfig)
