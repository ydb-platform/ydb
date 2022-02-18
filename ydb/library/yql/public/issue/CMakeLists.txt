add_library(yql-public-issue)
target_link_libraries(yql-public-issue PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  library-cpp-colorizer
  library-cpp-resource
  api-protos
  public-issue-protos
  library-yql-utils
  tools-enum_parser-enum_serialization_runtime
)
target_sources(yql-public-issue PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/yql_issue.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/yql_issue_message.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/yql_issue_manager.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/yql_issue_utils.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/yql_warning.cpp
)
generate_enum_serilization(yql-public-issue
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/issue/yql_warning.h
  INCLUDE_HEADERS
  ydb/library/yql/public/issue/yql_warning.h
)
