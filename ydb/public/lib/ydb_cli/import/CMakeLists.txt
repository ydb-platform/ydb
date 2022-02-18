add_library(lib-ydb_cli-import)
target_link_libraries(lib-ydb_cli-import PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
  common
  cpp-client-ydb_proto
)
target_sources(lib-ydb_cli-import PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/import/import.cpp
)
