_ydb_sdk_add_library(public-lib-yson_value)

target_link_libraries(public-lib-yson_value PUBLIC
  yutil
  yson
  client-ydb_result
  client-ydb_value
  library-uuid
)

target_sources(public-lib-yson_value PRIVATE
  ydb_yson_value.cpp
)

_ydb_sdk_make_client_component(YsonValue public-lib-yson_value)
