add_subdirectory(common)

_ydb_sdk_add_library(client-iam)

target_link_libraries(client-iam PUBLIC
  yutil
  api-client-yc_public
  client-iam-common
)

_ydb_sdk_install_targets(TARGETS client-iam)

target_sources(client-iam PRIVATE
  iam.cpp
)
