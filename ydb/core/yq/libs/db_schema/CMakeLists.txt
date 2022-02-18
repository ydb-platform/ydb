add_library(yq-libs-db_schema)
target_compile_options(yq-libs-db_schema PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-db_schema PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-client-ydb_params
  cpp-client-ydb_table
)
target_sources(yq-libs-db_schema PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/db_schema/db_schema.cpp
)
