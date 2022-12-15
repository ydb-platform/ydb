
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(proto)

add_library(core-file_storage-http_download)
target_link_libraries(core-file_storage-http_download PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-file_storage-defs
  core-file_storage-download
  core-file_storage-proto
  file_storage-http_download-proto
  yql-utils-fetch
  yql-utils-log
  library-yql-utils
  cpp-digest-md5
  cpp-http-misc
)
target_sources(core-file_storage-http_download PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/http_download/http_download.cpp
)
