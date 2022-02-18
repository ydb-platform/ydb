add_library(cpp-client-ydb_proto)
target_link_libraries(cpp-client-ydb_proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-grpc-draft
  api-protos
  lib-operation_id-protos
  public-issue-protos
)
target_sources(cpp-client-ydb_proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_proto/accessor.cpp
)
