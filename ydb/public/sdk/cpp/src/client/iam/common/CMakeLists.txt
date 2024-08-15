_ydb_sdk_add_library(client-iam-common)

target_link_libraries(client-iam-common PUBLIC
  yutil
  grpc-client
  http-simple
  json
  library-jwt
  client-ydb_types-credentials
  api-client-yc_public
)

target_sources(client-iam-common PRIVATE
  iam.cpp
)

_ydb_sdk_make_client_component(Iam client-iam-common)
