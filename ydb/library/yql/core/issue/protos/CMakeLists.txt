add_library(core-issue-protos)
target_link_libraries(core-issue-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  public-issue-protos
  contrib-libs-protobuf
)
target_proto_messages(core-issue-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/issue/protos/issue_id.proto
)
target_proto_plugin(core-issue-protos
  cpp_styleguide
  cpp_styleguide
)
