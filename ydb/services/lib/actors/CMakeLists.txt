add_library(services-lib-actors)
target_link_libraries(services-lib-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-grpc_services
  ydb-core-mind
  library-persqueue-obfuscate
  library-persqueue-topic_parser
  api-grpc
  api-grpc-draft
  public-lib-jwt
  public-lib-operation_id
)
target_sources(services-lib-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/lib/actors/pq_schema_actor.cpp
)
