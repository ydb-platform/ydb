_ydb_sdk_add_library(client-helpers)

target_link_libraries(client-helpers
  PUBLIC
    yutil
    client-ydb_types-credentials-oauth2
    client-iam
    client-ydb_types-credentials
)

target_sources(client-helpers
  PRIVATE
    helpers.cpp
)

_ydb_sdk_make_client_component(Helpers client-helpers)
