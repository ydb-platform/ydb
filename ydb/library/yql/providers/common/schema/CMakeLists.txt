add_library(providers-common-schema)
target_link_libraries(providers-common-schema PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yson-node
  library-yql-utils
)
target_sources(providers-common-schema PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/schema/yql_schema_utils.cpp
)
