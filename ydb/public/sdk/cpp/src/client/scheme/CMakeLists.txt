_ydb_sdk_add_library(client-ydb_scheme)

target_link_libraries(client-ydb_scheme PUBLIC
  yutil
  enum_serialization_runtime
  impl-ydb_internal-make_request
  client-ydb_common_client-impl
  client-ydb_driver
)

target_sources(client-ydb_scheme PRIVATE
  scheme.cpp
  out.cpp
)

generate_enum_serilization(client-ydb_scheme
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/scheme/scheme.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/scheme/scheme.h
)

_ydb_sdk_make_client_component(Scheme client-ydb_scheme)
