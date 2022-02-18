add_library(yql-dq-type_ann)
target_compile_options(yql-dq-type_ann PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-dq-type_ann PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  library-yql-core
  yql-dq-expr_nodes
  yql-dq-proto
)
target_sources(yql-dq-type_ann PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/type_ann/dq_type_ann.cpp
)
