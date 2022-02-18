add_library(common-schema-parser)
target_link_libraries(common-schema-parser PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yson-node
  library-cpp-yson
)
target_sources(common-schema-parser PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/schema/parser/yql_type_parser.cpp
)
