_ydb_sdk_add_library(client-iam_private)

target_link_libraries(client-iam_private
  PUBLIC
    client-iam-types
    yutil
  PRIVATE
    api-client-yc_private
    client-iam-common
)

target_sources(client-iam_private
  PRIVATE
    iam.cpp
)

_ydb_sdk_make_client_component(IamPrivate client-iam_private)
