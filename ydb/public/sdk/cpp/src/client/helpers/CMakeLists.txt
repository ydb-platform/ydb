_ydb_sdk_add_library(client-helpers)

target_link_libraries(client-helpers PUBLIC
  yutil
  client-iam-common
  client-ydb_types-credentials
  yql-public-issue-protos
)

target_sources(client-helpers PRIVATE
  helpers.cpp
)

_ydb_sdk_make_client_component(Helpers client-helpers)
