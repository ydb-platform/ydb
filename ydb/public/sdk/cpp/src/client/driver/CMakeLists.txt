_ydb_sdk_add_library(client-ydb_driver)

target_link_libraries(client-ydb_driver 
  PUBLIC
    client-ydb_common_client
    client-ydb_types-status
  PRIVATE
    impl-ydb_internal-common
    impl-ydb_internal-grpc_connections
    client-resources
)

target_sources(client-ydb_driver PRIVATE
  driver.cpp
)

_ydb_sdk_make_client_component(Driver client-ydb_driver)
