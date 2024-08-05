add_subdirectory(login)
add_subdirectory(oauth2_token_exchange)

_ydb_sdk_add_library(client-ydb_types-credentials)

target_link_libraries(client-ydb_types-credentials PUBLIC
  yutil
  api-grpc
  client-ydb_types-status
  yql-public-issue
)

target_sources(client-ydb_types-credentials PRIVATE
  credentials.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_types-credentials)
