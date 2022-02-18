add_library(yql-utils-actor_log)
target_link_libraries(yql-utils-actor_log PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-protos
  ydb-core-protos
)
target_sources(yql-utils-actor_log PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/actor_log/log.cpp
)
