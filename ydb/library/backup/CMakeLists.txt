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
