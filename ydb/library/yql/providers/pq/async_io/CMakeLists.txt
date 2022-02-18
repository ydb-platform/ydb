add_library(providers-pq-async_io)
target_compile_options(providers-pq-async_io PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-pq-async_io PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-minikql-computation
  common-token_accessor-client
  yql-public-types
  yql-utils-log
  cpp-client-ydb_driver
  cpp-client-ydb_persqueue_core
  client-ydb_types-credentials
  dq-actors-compute
  providers-pq-proto
)
target_sources(providers-pq-async_io PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/async_io/probes.cpp
)
