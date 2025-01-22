_ydb_sdk_add_library(client-draft)

target_link_libraries(client-draft PUBLIC
  yutil
  yql-public-issue
  api-grpc-draft
  client-ydb_table
  client-ydb_types-operation
  client-ydb_value
)

target_sources(client-draft PRIVATE
  ydb_dynamic_config.cpp
  ydb_replication.cpp
  ydb_scripting.cpp
  ydb_view.cpp
)

generate_enum_serilization(client-draft
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/draft/ydb_replication.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/draft/ydb_replication.h
)

_ydb_sdk_make_client_component(Draft client-draft)
