
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(client-ydb_persqueue-codecs)
target_link_libraries(client-ydb_persqueue-codecs PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-streams-lzop
  cpp-streams-zstd
  public-issue-protos
  api-grpc-draft
  api-protos
)
target_sources(client-ydb_persqueue-codecs PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/public/sdk/cpp/client/ydb_persqueue/codecs/codecs.cpp
)
