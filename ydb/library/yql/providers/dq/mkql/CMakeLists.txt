add_library(providers-dq-mkql)
target_compile_options(providers-dq-mkql PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-mkql PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-core
  providers-common-mkql
  providers-dq-expr_nodes
  providers-dq-interface
)
target_sources(providers-dq-mkql PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/mkql/dqs_mkql_compiler.cpp
)
