add_library(providers-s3-actors)
target_compile_options(providers-s3-actors PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-s3-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-minikql-computation
  common-token_accessor-client
  yql-public-types
  dq-actors-compute
  providers-common-http_gateway
  providers-s3-proto
)
target_sources(providers-s3-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/actors/yql_s3_read_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/actors/yql_s3_source_factory.cpp
)
