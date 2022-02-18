add_library(providers-common-structured_token)
target_link_libraries(providers-common-structured_token PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
  cpp-string_utils-base64
  library-yql-utils
)
target_sources(providers-common-structured_token PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/structured_token/yql_structured_token.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/structured_token/yql_token_builder.cpp
)
