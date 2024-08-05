add_subdirectory(common)

_ydb_sdk_add_library(client-iam)

target_link_libraries(client-iam
  PUBLIC
    client-iam-types
    yutil
  PRIVATE
    api-client-yc_public
    client-iam-common
    json
    http-simple
)

target_sources(client-iam PRIVATE
  iam.cpp
)

_ydb_sdk_make_client_component(Iam client-iam)
