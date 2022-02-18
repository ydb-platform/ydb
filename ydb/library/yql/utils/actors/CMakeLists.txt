add_library(yql-utils-actors)
target_link_libraries(yql-utils-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  common-token_accessor-client
  yql-public-types
  yql-public-udf
  providers-solomon-proto
)
target_sources(yql-utils-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/actors/rich_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/actors/http_sender_actor.cpp
)
