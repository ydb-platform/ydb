
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

add_library(kikimr_backup)
target_link_libraries(kikimr_backup PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-bucket_quoter
  cpp-regex-pcre
  cpp-string_utils-quote
  ydb-library-dynumber
  api-grpc
  api-protos
  ydb_cli-dump-util
  public-lib-yson_value
  cpp-client-ydb_proto
  cpp-client-ydb_scheme
  cpp-client-ydb_table
)
target_sources(kikimr_backup PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/backup/backup.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/backup/query_builder.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/backup/query_uploader.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/backup/util.cpp
)
