_ydb_sdk_add_library(public-lib-json_value)

target_link_libraries(public-lib-json_value PUBLIC
  yutil
  json-writer
  string_utils-base64
  client-ydb_result
  client-ydb_value
  library-uuid
)

target_sources(public-lib-json_value PRIVATE
  ydb_json_value.cpp
)

_ydb_sdk_make_client_component(JsonValue public-lib-json_value)
