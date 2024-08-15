_ydb_sdk_add_library(client-ydb_types-credentials-oauth2)

target_link_libraries(client-ydb_types-credentials-oauth2 PUBLIC
  yutil
  jwt-cpp::jwt-cpp
  cgiparam
  http-misc
  http-simple
  json
  retry
  uri
  client-ydb_types-credentials
  client-ydb_types
)

target_sources(client-ydb_types-credentials-oauth2 PRIVATE
  credentials.cpp
  jwt_token_source.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_types-credentials-oauth2)
