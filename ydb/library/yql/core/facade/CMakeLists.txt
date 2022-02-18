add_library(yql-core-facade)
target_compile_options(yql-core-facade PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-core-facade PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-split
  library-cpp-random_provider
  cpp-string_utils-base64
  cpp-threading-future
  library-cpp-time_provider
  library-cpp-yson
  cpp-yson-node
  yql-core-extract_predicate
  yql-core-file_storage
  yql-core-services
  library-yql-sql
  yql-utils-log
  library-yql-core
  yql-core-type_ann
  providers-common-config
  providers-common-proto
  providers-common-provider
  providers-common-udf_resolve
  yql-providers-config
  providers-result-provider
)
target_sources(yql-core-facade PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/facade/yql_facade.cpp
)
