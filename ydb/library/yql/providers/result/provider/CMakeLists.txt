add_library(providers-result-provider)
target_compile_options(providers-result-provider PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-result-provider PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-yson-node
  library-yql-ast
  yql-utils-log
  library-yql-core
  yql-core-expr_nodes
  providers-common-codec
  providers-common-mkql
  providers-common-provider
  providers-result-expr_nodes
)
target_sources(providers-result-provider PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/result/provider/yql_result_provider.cpp
)
