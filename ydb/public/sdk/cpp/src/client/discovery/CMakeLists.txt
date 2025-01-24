_ydb_sdk_add_library(client-ydb_discovery)

target_compile_options(client-ydb_discovery PRIVATE
  -Wno-deprecated
)

target_link_libraries(client-ydb_discovery PUBLIC
  yutil
  client-ydb_common_client-impl
  client-ydb_driver
)

target_sources(client-ydb_discovery PRIVATE
  discovery.cpp
)

_ydb_sdk_make_client_component(Discovery client-ydb_discovery)
