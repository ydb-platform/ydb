add_library(providers-solomon-async_io)
target_compile_options(providers-solomon-async_io PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-solomon-async_io PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-json-easy_parse
  monlib-encode-json
  yql-minikql-computation
  common-token_accessor-client
  yql-public-types
  yql-public-udf
  yql-utils-log
  dq-actors-compute
  providers-solomon-proto
)
target_sources(providers-solomon-async_io PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/solomon/async_io/dq_solomon_write_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/solomon/async_io/metrics_encoder.cpp
)
