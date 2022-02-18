add_library(providers-common-mkql)
target_compile_options(providers-common-mkql PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-common-mkql PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
  library-yql-ast
  library-yql-minikql
  yql-public-decimal
  yql-public-udf
  library-yql-utils
  library-yql-core
  yql-core-expr_nodes
  common-schema-expr
  providers-dq-expr_nodes
)
target_sources(providers-common-mkql PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/mkql/parser.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/mkql/yql_provider_mkql.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/mkql/yql_type_mkql.cpp
)
