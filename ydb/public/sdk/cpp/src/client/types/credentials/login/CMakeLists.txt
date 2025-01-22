_ydb_sdk_add_library(client-ydb_types-credentials-login)

target_link_libraries(client-ydb_types-credentials-login
  PUBLIC
    yutil
  PRIVATE
    api-grpc
    client-ydb_types-status
    impl-ydb_internal-grpc_connections
    yql-public-issue
    jwt-cpp::jwt-cpp
)

target_sources(client-ydb_types-credentials-login PRIVATE
  login.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_types-credentials-login)
