add_subdirectory(impl)
add_subdirectory(query_stats)

_ydb_sdk_add_library(client-ydb_table)

target_link_libraries(client-ydb_table PUBLIC
  yutil
  enum_serialization_runtime
  api-protos
  impl-ydb_internal-make_request
  impl-ydb_internal-kqp_session_common
  impl-ydb_internal-retry
  client-ydb_driver
  client-ydb_params
  client-ydb_proto
  client-ydb_result
  client-ydb_scheme
  client-ydb_table-impl
  client-ydb_table-query_stats
  client-ydb_types-operation
  client-ydb_value
  string_utils-misc
)

target_sources(client-ydb_table PRIVATE
  table.cpp
  proto_accessor.cpp
  out.cpp
)

generate_enum_serilization(client-ydb_table
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/table/table_enum.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/table/table_enum.h
)

_ydb_sdk_make_client_component(Table client-ydb_table)
