add_library(lib-ydb_cli-dump)
target_link_libraries(lib-ydb_cli-dump PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-bucket_quoter
  cpp-string_utils-quote
  kikimr_backup
  api-protos
  common
  ydb_cli-dump-util
  cpp-client-ydb_proto
)
target_sources(lib-ydb_cli-dump PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/dump/dump.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/dump/dump_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/dump/restore_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/dump/restore_import_data.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/dump/restore_compat.cpp
)
