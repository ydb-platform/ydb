add_library(ydb-core-grpc_streaming)
target_link_libraries(ydb-core-grpc_streaming PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  cpp-actors-core
  cpp-grpc-server
  ydb-core-base
)
target_sources(ydb-core-grpc_streaming PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_streaming/grpc_streaming.cpp
)
