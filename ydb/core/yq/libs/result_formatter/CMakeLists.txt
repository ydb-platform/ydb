add_library(yq-libs-result_formatter)
target_compile_options(yq-libs-result_formatter PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-result_formatter PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
  cpp-json-yson
  ydb-core-engine
  library-yql-ast
  library-yql-minikql
  yql-minikql-computation
  yql-public-udf
  api-protos
  cpp-client-ydb_proto
  cpp-client-ydb_result
  cpp-client-ydb_value
  providers-common-codec
  common-schema-expr
  common-schema-mkql
)
target_sources(yq-libs-result_formatter PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/result_formatter/result_formatter.cpp
)
