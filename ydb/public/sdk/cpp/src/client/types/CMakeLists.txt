add_subdirectory(credentials)
add_subdirectory(exceptions)
add_subdirectory(fatal_error_handlers)
add_subdirectory(operation)
add_subdirectory(status)

_ydb_sdk_add_library(client-ydb_types)

target_link_libraries(client-ydb_types PUBLIC
  yutil
  protobuf::libprotobuf
  grpc-client
  yql-public-issue
  enum_serialization_runtime
)

generate_enum_serilization(client-ydb_types
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/types/s3_settings.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/types/s3_settings.h
)

generate_enum_serilization(client-ydb_types
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/types/status_codes.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/types/status_codes.h
)

_ydb_sdk_make_client_component(Types client-ydb_types)
