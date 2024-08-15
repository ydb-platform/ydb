file(GLOB_RECURSE YQL_PUBLIC_ISSUE_PROTOS
  *.proto
)

_ydb_sdk_add_proto_library(yql-public-issue-protos
  SOURCES ${YQL_PUBLIC_ISSUE_PROTOS}
)

target_include_directories(yql-public-issue-protos PRIVATE
  ${YDB_SDK_BINARY_DIR}/ydb-cpp-sdk
)

_ydb_sdk_install_targets(TARGETS yql-public-issue-protos)
