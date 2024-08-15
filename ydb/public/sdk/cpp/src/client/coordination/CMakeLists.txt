_ydb_sdk_add_library(client-ydb_coordination)

target_link_libraries(client-ydb_coordination PUBLIC
  yutil
  enum_serialization_runtime
  api-grpc
  impl-ydb_internal-make_request
  client-ydb_common_client
  client-ydb_common_client-impl
  client-ydb_driver
  client-ydb_proto
  client-ydb_types
  client-ydb_types-status
)

target_sources(client-ydb_coordination PRIVATE
  coordination.cpp
  proto_accessor.cpp
)

generate_enum_serilization(client-ydb_coordination
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/coordination/coordination.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/coordination/coordination.h
)

_ydb_sdk_make_client_component(Coordination client-ydb_coordination)
