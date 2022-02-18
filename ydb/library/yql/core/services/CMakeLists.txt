add_library(yql-core-services)
target_compile_options(yql-core-services PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-core-services PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-string_utils-base64
  library-cpp-yson
  yql-ast-serialize
  library-yql-minikql
  library-yql-sql
  yql-utils-log
  library-yql-core
  yql-core-common_opt
  yql-core-peephole_opt
  yql-core-type_ann
  providers-common-codec
  providers-common-mkql
  providers-common-provider
  common-schema-expr
  providers-result-expr_nodes
)
target_sources(yql-core-services PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/services/yql_eval_expr.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/services/yql_eval_params.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/services/yql_out_transformers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/services/yql_plan.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/services/yql_transform_pipeline.cpp
)
