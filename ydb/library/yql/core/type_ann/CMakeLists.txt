add_library(yql-core-type_ann)
target_compile_options(yql-core-type_ann PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-core-type_ann PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  library-yql-minikql
  library-yql-utils
  yql-utils-log
  library-yql-core
  yql-core-expr_nodes
  yql-core-issue
  core-issue-protos
  common-schema-expr
  yql-parser-pg_catalog
)
target_sources(yql-core-type_ann PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/type_ann/type_ann_core.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/type_ann/type_ann_columnorder.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/type_ann/type_ann_expr.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/type_ann/type_ann_join.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/type_ann/type_ann_list.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/type_ann/type_ann_types.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/type_ann/type_ann_wide.cpp
)
