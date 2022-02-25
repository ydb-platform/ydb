add_library(public-issue-protos)
target_link_libraries(public-issue-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(public-issue-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/protos/issue_message.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/protos/issue_severity.proto
)
