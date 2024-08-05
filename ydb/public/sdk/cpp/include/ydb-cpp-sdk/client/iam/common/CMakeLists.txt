_ydb_sdk_add_library(client-iam-types INTERFACE)

target_link_libraries(client-iam-types
  INTERFACE
    client-ydb_types-credentials
    library-jwt
    yutil
)

_ydb_sdk_install_targets(client-iam-types)
