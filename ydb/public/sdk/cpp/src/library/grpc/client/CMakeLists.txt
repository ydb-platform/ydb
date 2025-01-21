_ydb_sdk_add_library(grpc-client)

target_link_libraries(grpc-client
  PUBLIC
    yutil
    gRPC::grpc++
)

target_sources(grpc-client PRIVATE
  grpc_client_low.cpp
)

target_compile_definitions(grpc-client PRIVATE YDB_DISABLE_GRPC_SOCKET_MUTATOR)

_ydb_sdk_install_targets(TARGETS grpc-client)
