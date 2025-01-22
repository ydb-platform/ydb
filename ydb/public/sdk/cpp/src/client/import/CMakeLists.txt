_ydb_sdk_add_library(client-ydb_import)

target_link_libraries(client-ydb_import PUBLIC
  yutil
  enum_serialization_runtime
  api-grpc
  api-protos
  client-ydb_common_client-impl
  client-ydb_driver
  client-ydb_proto
  client-ydb_types-operation
)

target_sources(client-ydb_import
  PRIVATE
    import.cpp
    out.cpp
)

generate_enum_serilization(client-ydb_import
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/import/import.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/import/import.h
)

_ydb_sdk_make_client_component(Import client-ydb_import)
