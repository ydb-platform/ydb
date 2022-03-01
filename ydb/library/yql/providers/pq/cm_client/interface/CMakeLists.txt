
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(pq-cm_client-interface)
target_link_libraries(pq-cm_client-interface PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-future
  client-ydb_types-credentials
  yql-public-issue
  tools-enum_parser-enum_serialization_runtime
)
target_sources(pq-cm_client-interface PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/cm_client/interface/client.cpp
)
generate_enum_serilization(pq-cm_client-interface
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/cm_client/interface/client.h
  INCLUDE_HEADERS
  ydb/library/yql/providers/pq/cm_client/interface/client.h
)
