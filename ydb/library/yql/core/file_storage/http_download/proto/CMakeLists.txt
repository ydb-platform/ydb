
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(file_storage-http_download-proto)
target_link_libraries(file_storage-http_download-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(file_storage-http_download-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/http_download/proto/http_download.proto
)
target_sources(file_storage-http_download-proto PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/file_storage/http_download/proto/http_download.pb.cc
)
