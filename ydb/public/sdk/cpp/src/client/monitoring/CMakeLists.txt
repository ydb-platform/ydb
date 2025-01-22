_ydb_sdk_add_library(client-ydb_monitoring)

target_link_libraries(client-ydb_monitoring PUBLIC
  yutil
  enum_serialization_runtime
  client-ydb_proto
  impl-ydb_internal-make_request
  client-ydb_common_client-impl
  client-ydb_driver
)

target_sources(client-ydb_monitoring PRIVATE
  monitoring.cpp
)

generate_enum_serilization(client-ydb_monitoring
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/monitoring/monitoring.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/monitoring/monitoring.h
)

_ydb_sdk_make_client_component(Monitoring client-ydb_monitoring)
