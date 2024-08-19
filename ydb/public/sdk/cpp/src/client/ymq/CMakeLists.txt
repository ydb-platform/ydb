_ydb_sdk_add_library(client-ymq)

target_link_libraries(client-ymq
  PUBLIC
    yutil
    grpc-client
    string_utils-url
    api-grpc-draft
    library-operation_id
    impl-ydb_internal-make_request
    client-ydb_driver 
)

target_sources(client-ymq
  PRIVATE
    ymq.cpp
)

_ydb_sdk_make_client_component(Ymq client-ymq)
