add_library(common-schema-mkql)
target_compile_options(common-schema-mkql PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(common-schema-mkql PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-yson
  cpp-yson-node
  library-yql-minikql
  yql-public-udf
  library-yql-utils
  providers-common-codec
  common-schema-parser
)
target_sources(common-schema-mkql PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.cpp
)
