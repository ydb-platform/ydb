_ydb_sdk_add_library(client-impl-ydb_endpoints)

target_link_libraries(client-impl-ydb_endpoints PUBLIC
  yutil
  monlib-metrics
  api-grpc
)

target_sources(client-impl-ydb_endpoints PRIVATE
  endpoints.cpp
)

_ydb_sdk_install_targets(TARGETS client-impl-ydb_endpoints)
