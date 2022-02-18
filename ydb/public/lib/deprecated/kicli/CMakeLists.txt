add_library(lib-deprecated-kicli)
target_link_libraries(lib-deprecated-kicli PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  cpp-actors-core
  cpp-threading-future
  ydb-core-protos
  ydb-library-aclib
  api-grpc
  api-grpc-draft
  api-protos
  public-lib-base
  lib-deprecated-client
  public-lib-scheme_types
  public-lib-value
  yql-public-decimal
  yql-public-issue
)
target_sources(lib-deprecated-kicli PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/kicli/configurator.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/kicli/dynamic_node.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/kicli/error.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/kicli/kikimr.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/kicli/query.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/kicli/result.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/kicli/schema.cpp
)
