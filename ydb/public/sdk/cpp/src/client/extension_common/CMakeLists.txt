_ydb_sdk_add_library(client-extension_common)

target_link_libraries(client-extension_common PUBLIC
  yutil
  monlib-metrics
  client-ydb_driver
)

target_sources(client-extension_common PRIVATE
  extension.cpp
)

_ydb_sdk_make_client_component(Extension client-extension_common)
