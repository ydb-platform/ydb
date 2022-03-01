
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-issue-protos)
target_link_libraries(core-issue-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  public-issue-protos
  contrib-libs-protobuf
)
target_proto_messages(core-issue-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/issue/protos/issue_id.proto
)
