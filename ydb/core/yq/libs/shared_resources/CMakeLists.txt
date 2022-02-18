add_library(yq-libs-shared_resources)
target_compile_options(yq-libs-shared_resources PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-shared_resources PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-monlib-dynamic_counters
  ydb-core-protos
  yq-libs-events
  libs-shared_resources-interface
  ydb-library-security
  cpp-client-ydb_driver
  cpp-client-ydb_table
)
target_sources(yq-libs-shared_resources PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/shared_resources/db_pool.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/shared_resources/shared_resources.cpp
)
