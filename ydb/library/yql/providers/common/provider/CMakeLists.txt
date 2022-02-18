add_library(providers-common-provider)
target_compile_options(providers-common-provider PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-common-provider PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-minikql
  yql-public-udf
  library-yql-sql
  library-yql-core
)
target_sources(providers-common-provider PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/provider/yql_data_provider_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/provider/yql_provider.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/provider/yql_table_lookup.cpp
)
