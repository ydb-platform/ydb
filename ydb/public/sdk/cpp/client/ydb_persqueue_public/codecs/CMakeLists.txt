add_library(client-ydb_persqueue_public-codecs)
target_link_libraries(client-ydb_persqueue_public-codecs PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-streams-zstd
  public-issue-protos
  api-grpc-draft
  api-protos
)
target_sources(client-ydb_persqueue_public-codecs PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs/codecs.cpp
)
