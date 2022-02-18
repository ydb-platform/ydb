add_library(ydb_cli-dump-util)
target_link_libraries(ydb_cli-dump-util PUBLIC
  contrib-libs-cxxsupp
  yutil
  common
  cpp-client-ydb_scheme
  cpp-client-ydb_table
  client-ydb_types-status
)
target_sources(ydb_cli-dump-util PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/dump/util/util.cpp
)
