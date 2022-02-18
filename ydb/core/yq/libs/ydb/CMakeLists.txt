add_library(yq-libs-ydb)
target_link_libraries(yq-libs-ydb PUBLIC
  contrib-libs-cxxsupp
  yutil
  yq-libs-config
  ydb-library-security
  cpp-client-ydb_scheme
  cpp-client-ydb_table
  tools-enum_parser-enum_serialization_runtime
)
target_sources(yq-libs-ydb PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/ydb/util.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/ydb/ydb.cpp
)
generate_enum_serilization(yq-libs-ydb
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/ydb/ydb.h
  INCLUDE_HEADERS
  ydb/core/yq/libs/ydb/ydb.h
)
