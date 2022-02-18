add_library(common-schema-skiff)
target_link_libraries(common-schema-skiff PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yson-node
  yql-public-udf
  providers-common-codec
  common-schema-parser
)
target_sources(common-schema-skiff PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/schema/skiff/yql_skiff_schema.cpp
)
