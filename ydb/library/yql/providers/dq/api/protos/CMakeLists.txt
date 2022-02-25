add_library(dq-api-protos)
target_link_libraries(dq-api-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-protos
  api-protos
  dq-actors-protos
  yql-dq-proto
  common-metrics-protos
  contrib-libs-protobuf
)
target_proto_messages(dq-api-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/api/protos/service.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/api/protos/dqs.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/api/protos/task_command_executor.proto
)
