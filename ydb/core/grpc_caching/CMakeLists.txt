add_library(ydb-core-grpc_caching)
target_link_libraries(ydb-core-grpc_caching PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-protos
  ydb-core-base
)
target_sources(ydb-core-grpc_caching PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_caching/cached_grpc_request_actor.cpp
)
