add_library(dq-actors-protos)
target_link_libraries(dq-actors-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-protos
  api-protos
  core-issue-protos
  yql-dq-proto
  public-issue-protos
  yql-public-types
  contrib-libs-protobuf
)
target_proto_messages(dq-actors-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/protos/dq_events.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/protos/dq_stats.proto
)
