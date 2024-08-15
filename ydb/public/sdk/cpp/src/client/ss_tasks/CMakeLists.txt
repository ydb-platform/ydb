_ydb_sdk_add_library(client-ss_tasks)

target_link_libraries(client-ss_tasks PUBLIC
  yutil
  api-grpc
  api-protos
  client-ydb_common_client-impl
  client-ydb_driver
  client-ydb_proto
  client-ydb_types-operation
)

target_sources(client-ss_tasks PRIVATE
  task.cpp
  out.cpp
)

generate_enum_serilization(client-ss_tasks
  ${YDB_SDK_SOURCE_DIR}/src/client/ss_tasks/task.h
  INCLUDE_HEADERS
  src/client/ss_tasks/task.h
)

_ydb_sdk_make_client_component(SSTasks client-ss_tasks)
