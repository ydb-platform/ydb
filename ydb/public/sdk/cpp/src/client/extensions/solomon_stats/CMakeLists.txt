_ydb_sdk_add_library(client-extensions-solomon_stats)

target_link_libraries(client-extensions-solomon_stats PUBLIC
  yutil
  monlib-encode-json
  monlib-metrics
  monlib-service
  monlib-service-pages
  client-extension_common
)

target_sources(client-extensions-solomon_stats PRIVATE
  pull_client.cpp
  pull_connector.cpp
)

_ydb_sdk_make_client_component(SolomonStats client-extensions-solomon_stats)
