add_library(yql-dq-proto)
target_link_libraries(yql-dq-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-protos
  contrib-libs-protobuf
)
target_proto_messages(yql-dq-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/proto/dq_checkpoint.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/proto/dq_state_load_plan.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/proto/dq_tasks.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/proto/dq_transport.proto
)
