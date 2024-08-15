add_subdirectory(protos)

_ydb_sdk_add_library(yql-public-issue)

target_link_libraries(yql-public-issue PUBLIC
  yutil
  protobuf::libprotobuf
  colorizer
  resource
  api-protos
  yql-public-issue-protos
  yql-utils
  enum_serialization_runtime
  string_utils-misc
)

target_sources(yql-public-issue PRIVATE
  yql_issue_id.cpp
  yql_issue_message.cpp
  yql_issue.cpp
  out.cpp
)

_ydb_sdk_install_targets(TARGETS yql-public-issue)
