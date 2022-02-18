add_library(tx-replication-ydb_proxy)
target_compile_options(tx-replication-ydb_proxy PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(tx-replication-ydb_proxy PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
  cpp-client-ydb_driver
  cpp-client-ydb_scheme
  cpp-client-ydb_table
  client-ydb_types-credentials
)
target_sources(tx-replication-ydb_proxy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/replication/ydb_proxy/ydb_proxy.cpp
)
