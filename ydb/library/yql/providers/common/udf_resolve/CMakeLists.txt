add_library(providers-common-udf_resolve)
target_compile_options(providers-common-udf_resolve PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-common-udf_resolve PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-protobuf-util
  yql-core-file_storage
  library-yql-minikql
  yql-public-udf
  library-yql-utils
  yql-utils-log
  library-yql-core
  providers-common-mkql
  providers-common-proto
  common-schema-expr
)
target_sources(providers-common-udf_resolve PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/udf_resolve/yql_files_box.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/udf_resolve/yql_outproc_udf_resolver.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/udf_resolve/yql_udf_resolver_with_index.cpp
)
