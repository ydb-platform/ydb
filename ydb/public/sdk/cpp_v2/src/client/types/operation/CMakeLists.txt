_ydb_sdk_add_library(client-ydb_types-operation)

target_link_libraries(client-ydb_types-operation PUBLIC
  yutil
  protobuf::libprotobuf
  threading-future
  library-operation_id
  client-ydb_types
)

target_sources(client-ydb_types-operation PRIVATE
  operation.cpp
  out.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_types-operation)
