_ydb_sdk_add_library(client-ydb_export)

target_link_libraries(client-ydb_export PUBLIC
  yutil
  enum_serialization_runtime
  api-grpc
  api-protos
  client-ydb_common_client-impl
  client-ydb_driver
  client-ydb_proto
  client-ydb_types-operation
)

target_sources(client-ydb_export PRIVATE
  export.cpp
  out.cpp
)

generate_enum_serilization(client-ydb_export
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/export/export.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/export/export.h
)

_ydb_sdk_make_client_component(Export client-ydb_export)
